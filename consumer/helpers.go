package consumer

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/deso-protocol/core/lib"
	"os"
	"reflect"
	"time"
)

// CopyStruct takes 2 structs and copies values from fields of the same name from the source struct to the destination struct.
// This is used to copy values from a deso entry struct to a protobuf entry struct.
func CopyStruct(src interface{}, dst interface{}) error {
	srcValue := reflect.ValueOf(src).Elem()
	dstValue := reflect.ValueOf(dst).Elem()

	if srcValue.Kind() != reflect.Struct || dstValue.Kind() != reflect.Struct {
		return fmt.Errorf("both srcValue and dst must be structs")
	}

	// Loop through all the fields in the source struct, and copy them over to the destination struct
	// if the destination struct contains a field of the same name and type.
	for i := 0; i < dstValue.NumField(); i++ {
		// Get properties of the source field.
		dstFieldName := dstValue.Type().Field(i).Name
		dstFieldType := dstValue.Type().Field(i).Type
		dstFieldDecodeFunction := dstValue.Type().Field(i).Tag.Get("decode_function")
		dstFieldDecodeSrcField := dstValue.Type().Field(i).Tag.Get("decode_src_field_name")
		srcField := srcValue.FieldByName(dstFieldName)
		dstField := dstValue.FieldByName(dstFieldName)

		// TODO: Break each of these out into their own functions.
		// If the field needs to be decoded in some way, handle that here.
		if dstFieldDecodeFunction == "blockhash" {
			fieldValue := srcValue.FieldByName(dstFieldDecodeSrcField)
			if fieldValue.IsValid() && fieldValue.Elem().IsValid() {
				postHashBytes := fieldValue.Elem().Slice(0, lib.HashSizeBytes).Bytes()
				dstValue.FieldByName(dstFieldName).SetString(hex.EncodeToString(postHashBytes))
			}
		} else if dstFieldDecodeFunction == "bytehash" {
			fieldValue := srcValue.FieldByName(dstFieldDecodeSrcField)
			if fieldValue.IsValid() && fieldValue.Len() > 0 {
				postHashBytes := fieldValue.Slice(0, lib.HashSizeBytes).Bytes()
				dstValue.FieldByName(dstFieldName).SetString(hex.EncodeToString(postHashBytes))
			}
		} else if dstFieldDecodeFunction == "deso_body_schema" {
			bodyField := srcValue.FieldByName(dstFieldDecodeSrcField)
			bodyBytes := bodyField.Bytes()
			var body lib.DeSoBodySchema
			err := json.Unmarshal(bodyBytes, &body)
			if err != nil {
				return err
			}

			dstValue.FieldByName(dstValue.Type().Field(i).Tag.Get("decode_body_field_name")).SetString(body.Body)
			dstValue.FieldByName(dstValue.Type().Field(i).Tag.Get("decode_image_urls_field_name")).Set(reflect.ValueOf(body.ImageURLs))
			dstValue.FieldByName(dstValue.Type().Field(i).Tag.Get("decode_video_urls_field_name")).Set(reflect.ValueOf(body.VideoURLs))
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

func DecodeEntry(encoder lib.DeSoEncoder, entryBytes []byte) error {
	if encoder == nil {
		return errors.New("Error getting encoder")
	}

	rr := bytes.NewReader(entryBytes)

	if exists, err := lib.DecodeFromBytes(encoder, rr); exists && err == nil {
		return nil
	} else {
		return errors.New("Error decoding entry")
	}
}

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

// utxoOpToEncoderAndOperationType returns the encoder and operation type for a given UtxoOperation. This allows the
// consumer to translate a UtxoOperation into whatever state change operation it should be performing (i.e. upsert or
// delete), and which values should be passed to that operation. The encoder type is also returned, because for any deletes,
// the encoder passed back will be nil, thus not allowing the consumer to determine the type of the encoder.
func utxoOpToEncoderAndOperationType(utxoOp *lib.UtxoOperation) (lib.DeSoEncoder, lib.StateSyncerOperationType, lib.EncoderType) {
	switch utxoOp.Type {
	case lib.OperationTypeSubmitPost:
		if utxoOp.PrevPostEntry == nil {
			return utxoOp.PrevPostEntry, lib.DbOperationTypeDelete, lib.EncoderTypePostEntry
		} else {
			return utxoOp.PrevPostEntry, lib.DbOperationTypeUpsert, lib.EncoderTypePostEntry
		}
	default:
		return nil, lib.DbOperationTypeSkip, lib.EncoderTypeUtxoEntry
	}
}
