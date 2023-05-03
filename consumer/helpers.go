package consumer

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/deso-protocol/core/lib"
	"github.com/holiman/uint256"
	"github.com/pkg/errors"
	"github.com/uptrace/bun/extra/bunbig"
	"os"
	"reflect"
	"time"
)

// CopyStruct takes 2 structs and copies values from fields of the same name from the source struct to the destination struct.
// This helper can be used by the data handler to easily copy values between the deso encoder and whichever struct type
// is needed to perform the db operations by the handler.
// This function also handles decoding fields that need to be decoded in some way. These fields are marked with a
// "decode_function" tag in the destination struct.
// The "decode_src_field_name" tag is used to specify the name of the source struct field that contains the data to be decoded.
// The "decode_body_field_name" tag is used to specify the name of the destination struct field that the decoded data should be copied to.
// The "decode_function" tag can be one of the following: "blockhash", "bytehash", "deso_body_schema", "base_58_check",
// "extra_data", and "timestamp".
func CopyStruct(src interface{}, dst interface{}) error {
	srcValue := reflect.ValueOf(src).Elem()
	dstValue := reflect.ValueOf(dst).Elem()

	if srcValue.Kind() != reflect.Struct || dstValue.Kind() != reflect.Struct {
		return fmt.Errorf("both srcValue and dst must be structs")
	}

	// Loop through all the fields in the source struct, and copy them over to the destination struct
	// if the destination struct contains a field of the same name and type.
	for ii := 0; ii < dstValue.NumField(); ii++ {
		// Get properties of the source field.
		dstFieldName := dstValue.Type().Field(ii).Name
		dstFieldType := dstValue.Type().Field(ii).Type
		dstFieldDecodeFunction := dstValue.Type().Field(ii).Tag.Get("decode_function")
		dstFieldDecodeSrcField := dstValue.Type().Field(ii).Tag.Get("decode_src_field_name")
		srcField := srcValue.FieldByName(dstFieldName)
		dstField := dstValue.FieldByName(dstFieldName)

		// TODO: Break each of these out into their own functions.
		// TODO: Create comprehensive documentation of the various decoder functions.
		// TODO: all these functions that convert from bytes to hex strings should be consolidated.
		// If the field needs to be decoded in some way, handle that here.
		if dstFieldDecodeFunction == "blockhash" {
			fieldValue := srcValue.FieldByName(dstFieldDecodeSrcField)
			if fieldValue.IsValid() && fieldValue.Elem().IsValid() {
				postHashBytes := fieldValue.Elem().Slice(0, lib.HashSizeBytes).Bytes()
				dstValue.FieldByName(dstFieldName).SetString(hex.EncodeToString(postHashBytes))
			}
		} else if dstFieldDecodeFunction == "group_key_name" {
			fieldValue := srcValue.FieldByName(dstFieldDecodeSrcField)
			if fieldValue.IsValid() && fieldValue.Elem().IsValid() {
				groupKeyNameBytes := fieldValue.Elem().Slice(0, lib.MaxAccessGroupKeyNameCharacters).Bytes()
				dstValue.FieldByName(dstFieldName).SetString(hex.EncodeToString(groupKeyNameBytes))
			}
		} else if dstFieldDecodeFunction == "pkid" {
			fieldValue := srcValue.FieldByName(dstFieldDecodeSrcField)
			if fieldValue.IsValid() && fieldValue.Elem().IsValid() {
				pkidBytes := fieldValue.Elem().Slice(0, lib.PublicKeyLenCompressed).Bytes()
				dstValue.FieldByName(dstFieldName).Set(reflect.ValueOf(pkidBytes))
			}
		} else if dstFieldDecodeFunction == "bytehash" {
			fieldValue := srcValue.FieldByName(dstFieldDecodeSrcField)
			if fieldValue.IsValid() && fieldValue.Len() > 0 {
				postHashBytes := fieldValue.Slice(0, lib.HashSizeBytes).Bytes()
				dstValue.FieldByName(dstFieldName).SetString(hex.EncodeToString(postHashBytes))
			}
		} else if dstFieldDecodeFunction == "uint256" {
			srcInt, ok := srcField.Interface().(uint256.Int)
			if !ok {
				return errors.New("could not convert src field to uint256.Int")
			}
			dstField.Set(reflect.ValueOf(bunbig.FromMathBig(srcInt.ToBig())))
		} else if dstFieldDecodeFunction == "deso_body_schema" {
			bodyField := srcValue.FieldByName(dstFieldDecodeSrcField)
			bodyBytes := bodyField.Bytes()
			var body lib.DeSoBodySchema
			err := json.Unmarshal(bodyBytes, &body)
			if err != nil {
				return err
			}

			dstValue.FieldByName(dstValue.Type().Field(ii).Tag.Get("decode_body_field_name")).SetString(body.Body)
			dstValue.FieldByName(dstValue.Type().Field(ii).Tag.Get("decode_image_urls_field_name")).Set(reflect.ValueOf(body.ImageURLs))
			dstValue.FieldByName(dstValue.Type().Field(ii).Tag.Get("decode_video_urls_field_name")).Set(reflect.ValueOf(body.VideoURLs))
		} else if dstFieldDecodeFunction == "string_bytes" {
			stringField := srcValue.FieldByName(dstFieldDecodeSrcField)
			stringBytes := stringField.Bytes()
			dstValue.FieldByName(dstFieldName).SetString(string(stringBytes))
		} else if dstFieldDecodeFunction == "nested_value" {
			structField := srcValue.FieldByName(dstFieldDecodeSrcField)
			if structField.IsValid() {
				dstValue.FieldByName(dstFieldName).Set(structField.FieldByName(dstValue.Type().Field(ii).Tag.Get("nested_field_name")))
			}
		} else if dstFieldDecodeFunction == "base_58_check" {
			fieldValue := srcValue.FieldByName(dstFieldDecodeSrcField)
			if fieldValue.IsValid() {
				// If syncing against testnet, these params should be changed.
				pkString := lib.PkToString(fieldValue.Bytes(), &lib.DeSoMainnetParams)
				dstValue.FieldByName(dstFieldName).SetString(pkString)
			}
		} else if dstFieldDecodeFunction == "extra_data" {
			extraData := srcValue.FieldByName(dstFieldDecodeSrcField)
			dstExtraData := dstValue.FieldByName(dstFieldName)

			if extraData.IsValid() && dstExtraData.IsValid() && dstExtraData.Kind() == reflect.Map && dstExtraData.Type().Elem().Kind() == reflect.String {
				newMap := reflect.MakeMap(dstExtraData.Type())

				for _, key := range extraData.MapKeys() {
					// Get the []byte value
					byteValue := extraData.MapIndex(key)

					// Convert the []byte value to string
					stringValue := string(byteValue.Bytes())

					// Add the string value to the new map
					newMap.SetMapIndex(key, reflect.ValueOf(stringValue))
				}
				// Set the PostExtraData field to the new map
				dstExtraData.Set(newMap)
			}
		} else if dstFieldDecodeFunction == "timestamp" {
			if srcValue.FieldByName(dstFieldDecodeSrcField).IsValid() {
				timestamp := time.Unix(0, int64(srcValue.FieldByName(dstFieldDecodeSrcField).Uint()))
				dstField.Set(reflect.ValueOf(timestamp))
			}
		}

		// If the field doesn't need to be decoded, just copy it over.
		if srcField.IsValid() && srcField.Type() == dstFieldType {
			dstField.Set(srcField)
		}
	}
	return nil
}

// Convert timestamp nanos to time.Time.
func UnixNanoToTime(unixNano uint64) time.Time {
	return time.Unix(0, int64(unixNano))
}

// Convert public key bytes to base58check string.
func PublicKeyBytesToBase58Check(publicKey []byte) string {
	// If running against testnet data, a different set of params should be used.
	return lib.PkToString(publicKey, &lib.DeSoMainnetParams)
}

// Convert public key bytes to base58check string.
func ConvertRoyaltyMapToByteStrings(royaltyMap map[lib.PKID]uint64) map[string]uint64 {
	newMap := make(map[string]uint64)
	for key, value := range royaltyMap {
		newMap[key.ToString()] = value
	}
	return newMap
}

func DecodeDesoBodySchema(bodyBytes []byte) (*lib.DeSoBodySchema, error) {
	var body lib.DeSoBodySchema
	err := json.Unmarshal(bodyBytes, &body)
	if err != nil {
		return nil, err
	}
	return &body, nil
}

func ExtraDataBytesToString(extraData map[string][]byte) map[string]string {
	newMap := make(map[string]string)
	for key, value := range extraData {
		newMap[key] = string(value)
	}
	return newMap
}

// DecodeEntry decodes bytes and returns a deso entry struct.
func DecodeEntry(encoder lib.DeSoEncoder, entryBytes []byte) error {
	if encoder == nil {
		return errors.New("Error getting encoder")
	}

	rr := bytes.NewReader(entryBytes)

	if _, err := lib.DecodeFromBytes(encoder, rr); err != nil {
		return errors.Wrapf(err, "Error decoding entry")
	}
	return nil
}

// getUint64FromFile reads the next 4 bytes from the stateChangeFile and returns a uint32.
func getUint32FromFile(file *os.File) (uint32, error) {
	// Read the contents of the next 4 bytes from the stateChangeFile into a byte slice.
	uint32Bytes, err := getBytesFromFile(4, file)
	if err != nil {
		return 0, err
	}

	// Use binary package to read a uint16 structSize from the byte slice representing the size of the following struct
	value := binary.LittleEndian.Uint32(uint32Bytes)
	return value, nil
}

// getBytesFromFile reads the next entryByteSize bytes from the stateChangeFile and returns a byte slice.
func getBytesFromFile(entryByteSize int, file *os.File) ([]byte, error) {
	// Read the contents of the entry from the stateChangeFile into a byte slice
	structBytes := make([]byte, entryByteSize)
	bytesRead, err := file.Read(structBytes)
	if err != nil {
		return nil, err
	}
	if bytesRead < entryByteSize {
		return nil, fmt.Errorf("Too few bytes read")
	}
	return structBytes, nil
}

func GetPKIDBytesFromKey(key []byte) []byte {
	prefixLen := len(lib.Prefixes.PrefixPKIDToProfileEntry)
	return key[prefixLen:]
}

// getDisconnectOperationTypeForPrevEntry returns the operation type for a given utxoOp entry in order to perform a mempool disconnect.
// If the encoder is nil, the operation type is delete. Otherwise, it is upsert.
func getDisconnectOperationTypeForPrevEntry(prevEntry lib.DeSoEncoder) lib.StateSyncerOperationType {
	// Use reflection to determine if the previous entry is nil.
	val := reflect.ValueOf(prevEntry)
	if val.Kind() == reflect.Ptr && val.IsNil() {
		// If the previous entry is nil, we should delete the current entry to revert it to its previous state.
		return lib.DbOperationTypeDelete
	} else {
		// If the previous entry isn't nil, an upsert will bring it back to its previous state.
		return lib.DbOperationTypeUpsert
	}
}
