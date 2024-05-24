package consumer

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/btcsuite/btcd/btcec"
	"github.com/deso-protocol/core/lib"
	"github.com/golang/glog"
	"github.com/holiman/uint256"
	"github.com/pkg/errors"
	"github.com/uptrace/bun/extra/bunbig"
	"os"
	"path/filepath"
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

func createDirAndFile(filePath string) (*os.File, error) {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, errors.Wrapf(err, "Error creating directory:")
	}
	return os.Create(filePath)
}

// Convert timestamp nanos to time.Time.
func UnixNanoToTime(unixNano uint64) time.Time {
	return time.Unix(0, int64(unixNano))
}

// Convert public key bytes to base58check string.
//
//	func PublicKeyBytesToBase58Check(publicKey []byte, params *lib.DeSoParams) string {
//		// If running against testnet data, a different set of params should be used.
//		return lib.PkToString(publicKey, params)
//	}
func PublicKeyBytesToBase58Check(publicKey []byte, params *lib.DeSoParams) string {
	// If running against testnet data, a different set of params should be used.
	return lib.PkToString(publicKey, params)
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
func getUint64FromFile(file *os.File) (uint64, error) {
	// Read the contents of the next 4 bytes from the stateChangeFile into a byte slice.
	uint64Bytes, err := getBytesFromFile(8, file)
	if err != nil {
		return 0, err
	}

	// Use binary package to read a uint16 structSize from the byte slice representing the size of the following struct
	value := binary.LittleEndian.Uint64(uint64Bytes)
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
	if len(key) < len(lib.Prefixes.PrefixPKIDToProfileEntry) {
		return nil
	}
	prefixLen := len(lib.Prefixes.PrefixPKIDToProfileEntry)
	return key[prefixLen:]
}

func GetAccessGroupMemberFieldsFromKey(key []byte) (accessGroupMemberPublicKey []byte, accessGroupOwnerPublicKey []byte, accessGroupKeyName []byte, err error) {

	prefixLen := len(lib.Prefixes.PrefixAccessGroupMembershipIndex)
	totalKeyLen := prefixLen + lib.PublicKeyLenCompressed*2 + lib.MaxAccessGroupKeyNameCharacters

	if len(key) < totalKeyLen {
		return nil, nil, nil, errors.New("key length is less than expected")
	}

	accessGroupMemberPublicKey = key[prefixLen : prefixLen+lib.PublicKeyLenCompressed]
	accessGroupOwnerPublicKey = key[prefixLen+lib.PublicKeyLenCompressed : prefixLen+lib.PublicKeyLenCompressed*2]
	accessGroupKeyName = key[prefixLen+lib.PublicKeyLenCompressed*2 : totalKeyLen]

	return accessGroupMemberPublicKey, accessGroupOwnerPublicKey, accessGroupKeyName, nil
}

func GetBlockHashBytesFromKey(key []byte) []byte {
	if len(key) < len(lib.Prefixes.PrefixBlockHashToBlock) {
		return nil
	}
	prefixLen := len(lib.Prefixes.PrefixBlockHashToBlock)
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

func ComputeTransactionMetadata(txn *lib.MsgDeSoTxn, blockHashHex string, params *lib.DeSoParams,
	fees uint64, txnIndexInBlock uint64, utxoOps []*lib.UtxoOperation) (*lib.TransactionMetadata, error) {

	var err error
	txnMeta := &lib.TransactionMetadata{
		TxnIndexInBlock: txnIndexInBlock,
		TxnType:         txn.TxnMeta.GetTxnType().String(),

		// This may be overwritten later on, for example if we're dealing with a
		// BitcoinExchange txn which doesn't set the txn.PublicKey
		TransactorPublicKeyBase58Check: lib.PkToString(txn.PublicKey, params),

		// General transaction metadata
		BasicTransferTxindexMetadata: &lib.BasicTransferTxindexMetadata{
			FeeNanos: fees,
			// TODO: This doesn't add much value, and it makes output hard to read because
			// it's so long so I'm commenting it out for now.
			//UtxoOpsDump:      spew.Sdump(utxoOps),

			// We need to include the utxoOps because it allows us to compute implicit
			// outputs.
			UtxoOps: utxoOps,
		},

		TxnOutputs: txn.TxOutputs,
	}

	if blockHashHex != "" {
		txnMeta.BlockHashHex = blockHashHex
	}

	extraData := txn.ExtraData

	// Set the affected public keys for the basic transfer.
	for _, output := range txn.TxOutputs {
		txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
			PublicKeyBase58Check: lib.PkToString(output.PublicKey, params),
			Metadata:             "BasicTransferOutput",
		})
	}

	switch txn.TxnMeta.GetTxnType() {
	case lib.TxnTypeCreatorCoin:
		// Get the txn metadata
		realTxMeta := txn.TxnMeta.(*lib.CreatorCoinMetadataa)

		utxoOp := getUtxoOpByOperationType(utxoOps, lib.OperationTypeCreatorCoin)
		if utxoOp == nil || utxoOp.StateChangeMetadata == nil {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing creator coin utxo op error: %v", txn.Hash().String())
		}

		stateChangeMetadata, ok := utxoOp.StateChangeMetadata.(*lib.CreatorCoinStateChangeMetadata)
		if !ok {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing creator coin state change metadata error: %v", txn.Hash().String())
		}

		// Rosetta needs to know the change in DESOLockedNanos so it can model the change in
		// total deso locked in the creator coin. Calculate this by comparing the current CreatorCoinEntry
		// to the previous CreatorCoinEntry
		prevCoinEntry := utxoOp.PrevCoinEntry

		desoLockedNanosDiff := int64(0)
		if prevCoinEntry == nil {
			glog.Errorf("Update TxIndex: missing DESOLockedNanosDiff error: %v", txn.Hash().String())
		} else {
			desoLockedNanosDiff = int64(stateChangeMetadata.ProfileDeSoLockedNanos - prevCoinEntry.DeSoLockedNanos)
		}

		// Set the amount of the buy/sell/add
		txnMeta.CreatorCoinTxindexMetadata = &lib.CreatorCoinTxindexMetadata{
			DeSoToSellNanos:        realTxMeta.DeSoToSellNanos,
			CreatorCoinToSellNanos: realTxMeta.CreatorCoinToSellNanos,
			DeSoToAddNanos:         realTxMeta.DeSoToAddNanos,
			DESOLockedNanosDiff:    desoLockedNanosDiff,
		}

		// Set the type of the operation.
		if realTxMeta.OperationType == lib.CreatorCoinOperationTypeBuy {
			txnMeta.CreatorCoinTxindexMetadata.OperationType = "buy"
		} else if realTxMeta.OperationType == lib.CreatorCoinOperationTypeSell {
			txnMeta.CreatorCoinTxindexMetadata.OperationType = "sell"
		} else {
			txnMeta.CreatorCoinTxindexMetadata.OperationType = "add"
		}

		// Set the affected public key to the owner of the creator coin so that they
		// get notified.
		txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
			PublicKeyBase58Check: lib.PkToString(realTxMeta.ProfilePublicKey, params),
			Metadata:             "CreatorPublicKey",
		})
	case lib.TxnTypeCreatorCoinTransfer:
		realTxMeta := txn.TxnMeta.(*lib.CreatorCoinTransferMetadataa)
		utxoOp := getUtxoOpByOperationType(utxoOps, lib.OperationTypeCreatorCoinTransfer)
		if utxoOp == nil || utxoOp.StateChangeMetadata == nil {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing cc transfer utxo op error: %v", txn.Hash().String())
		}

		stateChangeMetadata, ok := utxoOp.StateChangeMetadata.(*lib.CCTransferStateChangeMetadata)
		if !ok {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing cc transfer state change metadata error: %v", txn.Hash().String())
		}

		txnMeta.CreatorCoinTransferTxindexMetadata = &lib.CreatorCoinTransferTxindexMetadata{
			CreatorUsername:            string(stateChangeMetadata.CreatorProfileEntry.Username),
			CreatorCoinToTransferNanos: realTxMeta.CreatorCoinToTransferNanos,
		}

		diamondLevelBytes, hasDiamondLevel := txn.ExtraData[lib.DiamondLevelKey]
		diamondPostHash, hasDiamondPostHash := txn.ExtraData[lib.DiamondPostHashKey]
		if hasDiamondLevel && hasDiamondPostHash {
			diamondLevel, bytesRead := lib.Varint(diamondLevelBytes)
			if bytesRead <= 0 {
				glog.Errorf("Update TxIndex: Error reading diamond level for txn: %v", txn.Hash().String())
			} else {
				txnMeta.CreatorCoinTransferTxindexMetadata.DiamondLevel = diamondLevel
				txnMeta.CreatorCoinTransferTxindexMetadata.PostHashHex = hex.EncodeToString(diamondPostHash)
			}
		}

		txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
			PublicKeyBase58Check: lib.PkToString(realTxMeta.ReceiverPublicKey, params),
			Metadata:             "ReceiverPublicKey",
		})
	case lib.TxnTypeUpdateProfile:
		realTxMeta := txn.TxnMeta.(*lib.UpdateProfileMetadata)

		txnMeta.UpdateProfileTxindexMetadata = &lib.UpdateProfileTxindexMetadata{}
		if len(realTxMeta.ProfilePublicKey) == btcec.PubKeyBytesLenCompressed {
			txnMeta.UpdateProfileTxindexMetadata.ProfilePublicKeyBase58Check =
				lib.PkToString(realTxMeta.ProfilePublicKey, params)
		}
		txnMeta.UpdateProfileTxindexMetadata.NewUsername = string(realTxMeta.NewUsername)
		txnMeta.UpdateProfileTxindexMetadata.NewDescription = string(realTxMeta.NewDescription)
		txnMeta.UpdateProfileTxindexMetadata.NewProfilePic = string(realTxMeta.NewProfilePic)
		txnMeta.UpdateProfileTxindexMetadata.NewCreatorBasisPoints = realTxMeta.NewCreatorBasisPoints
		txnMeta.UpdateProfileTxindexMetadata.NewStakeMultipleBasisPoints = realTxMeta.NewStakeMultipleBasisPoints
		txnMeta.UpdateProfileTxindexMetadata.IsHidden = realTxMeta.IsHidden

		// Add the ProfilePublicKey to the AffectedPublicKeys
		txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
			PublicKeyBase58Check: lib.PkToString(realTxMeta.ProfilePublicKey, params),
			Metadata:             "ProfilePublicKeyBase58Check",
		})
	case lib.TxnTypeSubmitPost:
		realTxMeta := txn.TxnMeta.(*lib.SubmitPostMetadata)
		utxoOp := getUtxoOpByOperationType(utxoOps, lib.OperationTypeSubmitPost)
		if utxoOp == nil || utxoOp.StateChangeMetadata == nil {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing submit post utxo op error: %v", txn.Hash().String())
		}

		stateChangeMetadata, ok := utxoOp.StateChangeMetadata.(*lib.SubmitPostStateChangeMetadata)
		if !ok {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing submit post state change metadata error: %v", txn.Hash().String())
		}

		txnMeta.SubmitPostTxindexMetadata = &lib.SubmitPostTxindexMetadata{}
		if len(realTxMeta.PostHashToModify) == lib.HashSizeBytes {
			txnMeta.SubmitPostTxindexMetadata.PostHashBeingModifiedHex = hex.EncodeToString(
				realTxMeta.PostHashToModify)
		}
		if len(realTxMeta.ParentStakeID) == lib.HashSizeBytes {
			txnMeta.SubmitPostTxindexMetadata.ParentPostHashHex = hex.EncodeToString(
				realTxMeta.ParentStakeID)
		}
		// If a post hash didn't get set then the hash of the transaction itself will
		// end up being used as the post hash so set that here.
		if txnMeta.SubmitPostTxindexMetadata.PostHashBeingModifiedHex == "" {
			txnMeta.SubmitPostTxindexMetadata.PostHashBeingModifiedHex =
				hex.EncodeToString(txn.Hash()[:])
		}

		// If ParentPostHashHex is set then get the parent posts public key and
		// mark it as affected. We only check this if PostHashToModify is not set
		// so we only generate a notification the first time someone comments on your post.
		// ParentPosterPublicKeyBase58Check is in AffectedPublicKeys
		if len(realTxMeta.PostHashToModify) == 0 && len(realTxMeta.ParentStakeID) == lib.HashSizeBytes {
			postHash := &lib.BlockHash{}
			copy(postHash[:], realTxMeta.ParentStakeID)
			postEntry := stateChangeMetadata.PostEntry
			if postEntry == nil {
				glog.V(2).Infof(
					"UpdateTxindex: Error creating SubmitPostTxindexMetadata; "+
						"missing parent post for hash %v: %v", postHash, err)
			} else {
				txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
					PublicKeyBase58Check: lib.PkToString(postEntry.PosterPublicKey, params),
					Metadata:             "ParentPosterPublicKeyBase58Check",
				})
			}
		}

		// The profiles that are mentioned are in the AffectedPublicKeys
		// MentionedPublicKeyBase58Check in AffectedPublicKeys. We need to
		// parse them out of the post and then look up their public keys.
		//
		// Start by trying to parse the body JSON
		bodyObj := &lib.DeSoBodySchema{}
		if err = json.Unmarshal(realTxMeta.Body, &bodyObj); err != nil {
			// Don't worry about bad posts unless we're debugging with high verbosity.
			glog.V(2).Infof("UpdateTxindex: Error parsing post body for @ mentions: "+
				"%v %v", string(realTxMeta.Body), err)
		} else {
			for _, mentionedProfile := range stateChangeMetadata.ProfilesMentioned {
				txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
					PublicKeyBase58Check: lib.PkToString(mentionedProfile.PublicKey, params),
					Metadata:             "MentionedPublicKeyBase58Check",
				})
			}

			// Additionally, we need to check if this post is a repost and
			// fetch the original poster
			if repostedPostHash, isRepost := extraData[lib.RepostedPostHash]; isRepost {
				repostedBlockHash := &lib.BlockHash{}
				copy(repostedBlockHash[:], repostedPostHash)
				// TODO: How to get this
				repostPost := stateChangeMetadata.RepostPostEntry
				if repostPost != nil {
					txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
						PublicKeyBase58Check: lib.PkToString(repostPost.PosterPublicKey, params),
						Metadata:             "RepostedPublicKeyBase58Check",
					})
				}
			}
		}
	case lib.TxnTypeLike:
		realTxMeta := txn.TxnMeta.(*lib.LikeMetadata)

		utxoOp := getUtxoOpByOperationType(utxoOps, lib.OperationTypeLike)
		if utxoOp == nil || utxoOp.StateChangeMetadata == nil {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing like utxo op error: %v", txn.Hash().String())
		}

		stateChangeMetadata, ok := utxoOp.StateChangeMetadata.(*lib.LikeStateChangeMetadata)
		if !ok {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing like state change metadata error: %v", txn.Hash().String())
		}

		txnMeta.LikeTxindexMetadata = &lib.LikeTxindexMetadata{
			IsUnlike:    realTxMeta.IsUnlike,
			PostHashHex: hex.EncodeToString(realTxMeta.LikedPostHash[:]),
		}

		// Get the public key of the poster and set it as having been affected
		// by this like.
		//
		// PosterPublicKeyBase58Check in AffectedPublicKeys
		postHash := &lib.BlockHash{}
		copy(postHash[:], realTxMeta.LikedPostHash[:])
		// TODO: How to get this.
		postEntry := stateChangeMetadata.LikedPostEntry
		if postEntry == nil {
			glog.V(2).Infof(
				"UpdateTxindex: Error creating LikeTxindexMetadata; "+
					"missing post for hash %v: %v", postHash, err)
		} else {
			txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
				PublicKeyBase58Check: lib.PkToString(postEntry.PosterPublicKey, params),
				Metadata:             "PosterPublicKeyBase58Check",
			})
		}
	case lib.TxnTypeFollow:
		realTxMeta := txn.TxnMeta.(*lib.FollowMetadata)

		txnMeta.FollowTxindexMetadata = &lib.FollowTxindexMetadata{
			IsUnfollow: realTxMeta.IsUnfollow,
		}

		// FollowedPublicKeyBase58Check in AffectedPublicKeys
		txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
			PublicKeyBase58Check: lib.PkToString(realTxMeta.FollowedPublicKey, params),
			Metadata:             "FollowedPublicKeyBase58Check",
		})
	case lib.TxnTypePrivateMessage:
		realTxMeta := txn.TxnMeta.(*lib.PrivateMessageMetadata)

		txnMeta.PrivateMessageTxindexMetadata = &lib.PrivateMessageTxindexMetadata{
			TimestampNanos: realTxMeta.TimestampNanos,
		}

		// RecipientPublicKeyBase58Check in AffectedPublicKeys
		txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
			PublicKeyBase58Check: lib.PkToString(realTxMeta.RecipientPublicKey, params),
			Metadata:             "RecipientPublicKeyBase58Check",
		})
	case lib.TxnTypeSwapIdentity:
		realTxMeta := txn.TxnMeta.(*lib.SwapIdentityMetadataa)

		utxoOp := getUtxoOpByOperationType(utxoOps, lib.OperationTypeSwapIdentity)
		if utxoOp == nil || utxoOp.StateChangeMetadata == nil {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing swap identity utxo op error: %v", txn.Hash().String())
		}

		stateChangeMetadata, ok := utxoOp.StateChangeMetadata.(*lib.SwapIdentityStateChangeMetadata)
		if !ok {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing swap identity state change metadata error: %v", txn.Hash().String())
		}

		// Rosetta needs to know the current locked deso in each profile so it can model the swap of
		// the creator coins. Rosetta models a swap identity as two INPUTs and two OUTPUTs effectively
		// swapping the balances of total deso locked. If no profile exists, from/to is zero.
		fromNanos := uint64(0)
		fromProfile := stateChangeMetadata.FromProfile
		if fromProfile != nil {
			fromNanos = fromProfile.CreatorCoinEntry.DeSoLockedNanos
		}

		toNanos := uint64(0)
		toProfile := stateChangeMetadata.ToProfile
		if toProfile != nil {
			toNanos = toProfile.CreatorCoinEntry.DeSoLockedNanos
		}

		txnMeta.SwapIdentityTxindexMetadata = &lib.SwapIdentityTxindexMetadata{
			FromPublicKeyBase58Check: lib.PkToString(realTxMeta.FromPublicKey, params),
			ToPublicKeyBase58Check:   lib.PkToString(realTxMeta.ToPublicKey, params),
			FromDeSoLockedNanos:      fromNanos,
			ToDeSoLockedNanos:        toNanos,
		}

		// The to and from public keys are affected by this.

		txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
			PublicKeyBase58Check: lib.PkToString(realTxMeta.FromPublicKey, params),
			Metadata:             "FromPublicKeyBase58Check",
		})
		txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
			PublicKeyBase58Check: lib.PkToString(realTxMeta.ToPublicKey, params),
			Metadata:             "ToPublicKeyBase58Check",
		})
	case lib.TxnTypeNFTBid:
		realTxMeta := txn.TxnMeta.(*lib.NFTBidMetadata)

		isBuyNow := false

		utxoOp := getUtxoOpByOperationType(utxoOps, lib.OperationTypeNFTBid)
		if utxoOp == nil || utxoOp.StateChangeMetadata == nil {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing nft bid utxo op error: %v", txn.Hash().String())
		}
		var nftRoyaltiesMetadata lib.NFTRoyaltiesMetadata
		var ownerPublicKeyBase58Check string
		var creatorPublicKeyBase58Check string
		// We don't send notifications for standing offers.
		if realTxMeta.SerialNumber != 0 {
			stateChangeMetadata, ok := utxoOp.StateChangeMetadata.(*lib.NFTBidStateChangeMetadata)
			if !ok {
				return nil, fmt.Errorf("ComputeTransactionMetadata: missing nft bid state change metadata error: %v", txn.Hash().String())
			}
			postEntry := stateChangeMetadata.PostEntry

			creatorPublicKeyBase58Check = lib.PkToString(postEntry.PosterPublicKey, params)

			if utxoOp.PrevNFTEntry != nil && utxoOp.PrevNFTEntry.IsBuyNow {
				isBuyNow = true
			}

			ownerPublicKeyBase58Check = stateChangeMetadata.OwnerPublicKeyBase58Check

			txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
				PublicKeyBase58Check: ownerPublicKeyBase58Check,
				Metadata:             "NFTOwnerPublicKeyBase58Check",
			})

			if isBuyNow {
				nftRoyaltiesMetadata = lib.NFTRoyaltiesMetadata{
					CreatorCoinRoyaltyNanos:     utxoOp.NFTBidCreatorRoyaltyNanos,
					CreatorRoyaltyNanos:         utxoOp.NFTBidCreatorDESORoyaltyNanos,
					CreatorPublicKeyBase58Check: creatorPublicKeyBase58Check,
					AdditionalCoinRoyaltiesMap: lib.PubKeyRoyaltyPairToBase58CheckToRoyaltyNanosMap(
						utxoOp.NFTBidAdditionalCoinRoyalties, params),
					AdditionalDESORoyaltiesMap: lib.PubKeyRoyaltyPairToBase58CheckToRoyaltyNanosMap(
						utxoOp.NFTBidAdditionalDESORoyalties, params),
				}
			}
		}

		txnMeta.NFTBidTxindexMetadata = &lib.NFTBidTxindexMetadata{
			NFTPostHashHex:            hex.EncodeToString(realTxMeta.NFTPostHash[:]),
			SerialNumber:              realTxMeta.SerialNumber,
			BidAmountNanos:            realTxMeta.BidAmountNanos,
			IsBuyNowBid:               isBuyNow,
			NFTRoyaltiesMetadata:      &nftRoyaltiesMetadata,
			OwnerPublicKeyBase58Check: ownerPublicKeyBase58Check,
		}

		if isBuyNow {
			txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
				PublicKeyBase58Check: creatorPublicKeyBase58Check,
				Metadata:             "NFTCreatorPublicKeyBase58Check",
			})

			for pubKeyIter, amountNanos := range txnMeta.NFTBidTxindexMetadata.NFTRoyaltiesMetadata.AdditionalCoinRoyaltiesMap {
				pubKey := pubKeyIter
				// Skip affected pub key if no royalty received
				if amountNanos == 0 {
					continue
				}
				txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
					PublicKeyBase58Check: pubKey,
					Metadata:             "AdditionalNFTRoyaltyToCreatorPublicKeyBase58Check",
				})
			}

			for pubKeyIter, amountNanos := range txnMeta.NFTBidTxindexMetadata.NFTRoyaltiesMetadata.AdditionalDESORoyaltiesMap {
				pubKey := pubKeyIter
				// Skip affected pub key if no royalty received
				if amountNanos == 0 {
					continue
				}
				txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
					PublicKeyBase58Check: pubKey,
					Metadata:             "AdditionalNFTRoyaltyToCoinPublicKeyBase58Check",
				})
			}
		}
	case lib.TxnTypeAcceptNFTBid:
		realTxMeta := txn.TxnMeta.(*lib.AcceptNFTBidMetadata)

		utxoOp := getUtxoOpByOperationType(utxoOps, lib.OperationTypeAcceptNFTBid)
		if utxoOp == nil || utxoOp.StateChangeMetadata == nil {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing accept bid utxo op error: %v", txn.Hash().String())
		}
		stateChangeMetadata, ok := utxoOp.StateChangeMetadata.(*lib.AcceptNFTBidStateChangeMetadata)
		if !ok {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing accept bid state change metadata error: %v", txn.Hash().String())
		}

		creatorPublicKeyBase58Check := lib.PkToString(utxoOp.PrevPostEntry.PosterPublicKey, params)

		txnMeta.AcceptNFTBidTxindexMetadata = &lib.AcceptNFTBidTxindexMetadata{
			NFTPostHashHex: hex.EncodeToString(realTxMeta.NFTPostHash[:]),
			SerialNumber:   realTxMeta.SerialNumber,
			BidAmountNanos: realTxMeta.BidAmountNanos,
			NFTRoyaltiesMetadata: &lib.NFTRoyaltiesMetadata{
				CreatorCoinRoyaltyNanos:     utxoOp.AcceptNFTBidCreatorRoyaltyNanos,
				CreatorRoyaltyNanos:         utxoOp.AcceptNFTBidCreatorDESORoyaltyNanos,
				CreatorPublicKeyBase58Check: creatorPublicKeyBase58Check,
				AdditionalCoinRoyaltiesMap: lib.PubKeyRoyaltyPairToBase58CheckToRoyaltyNanosMap(
					utxoOp.AcceptNFTBidAdditionalCoinRoyalties, params),
				AdditionalDESORoyaltiesMap: lib.PubKeyRoyaltyPairToBase58CheckToRoyaltyNanosMap(
					utxoOp.AcceptNFTBidAdditionalDESORoyalties, params),
			},
		}

		txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
			PublicKeyBase58Check: stateChangeMetadata.BidderPublicKeyBase58Check,
			Metadata:             "NFTBidderPublicKeyBase58Check",
		})

		txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
			PublicKeyBase58Check: creatorPublicKeyBase58Check,
			Metadata:             "NFTCreatorPublicKeyBase58Check",
		})

		for pubKeyIter, amountNanos := range txnMeta.AcceptNFTBidTxindexMetadata.NFTRoyaltiesMetadata.AdditionalCoinRoyaltiesMap {
			pubKey := pubKeyIter
			// Skip affected pub key if no royalty received
			if amountNanos == 0 {
				continue
			}
			txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
				PublicKeyBase58Check: pubKey,
				Metadata:             "AdditionalNFTRoyaltyToCreatorPublicKeyBase58Check",
			})
		}

		for pubKeyIter, amountNanos := range txnMeta.AcceptNFTBidTxindexMetadata.NFTRoyaltiesMetadata.AdditionalDESORoyaltiesMap {
			pubKey := pubKeyIter
			// Skip affected pub key if no royalty received
			if amountNanos == 0 {
				continue
			}
			txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
				PublicKeyBase58Check: pubKey,
				Metadata:             "AdditionalNFTRoyaltyToCoinPublicKeyBase58Check",
			})
		}
	case lib.TxnTypeCreateNFT:
		realTxMeta := txn.TxnMeta.(*lib.CreateNFTMetadata)

		utxoOp := getUtxoOpByOperationType(utxoOps, lib.OperationTypeCreateNFT)
		if utxoOp == nil || utxoOp.StateChangeMetadata == nil {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing create nft utxo op error: %v", txn.Hash().String())
		}
		stateChangeMetadata, ok := utxoOp.StateChangeMetadata.(*lib.CreateNFTStateChangeMetadata)
		if !ok {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing create nft state change metadata error: %v", txn.Hash().String())
		}

		additionalDESORoyaltiesMap := stateChangeMetadata.AdditionalDESORoyaltiesMap
		additionalCoinRoyaltiesMap := stateChangeMetadata.AdditionalCoinRoyaltiesMap
		txnMeta.CreateNFTTxindexMetadata = &lib.CreateNFTTxindexMetadata{
			NFTPostHashHex:             hex.EncodeToString(realTxMeta.NFTPostHash[:]),
			AdditionalDESORoyaltiesMap: additionalDESORoyaltiesMap,
			AdditionalCoinRoyaltiesMap: additionalCoinRoyaltiesMap,
		}
		for pubKeyIter := range additionalDESORoyaltiesMap {
			pubKey := pubKeyIter
			txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
				PublicKeyBase58Check: pubKey,
				Metadata:             "AdditionalNFTRoyaltyToCreatorPublicKeyBase58Check",
			})
		}
		for pubKeyIter := range additionalCoinRoyaltiesMap {
			pubKey := pubKeyIter
			txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
				PublicKeyBase58Check: pubKey,
				Metadata:             "AdditionalNFTRoyaltyToCoinPublicKeyBase58Check",
			})
		}
	case lib.TxnTypeUpdateNFT:
		realTxMeta := txn.TxnMeta.(*lib.UpdateNFTMetadata)

		utxoOp := getUtxoOpByOperationType(utxoOps, lib.OperationTypeUpdateNFT)
		if utxoOp == nil || utxoOp.StateChangeMetadata == nil {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing update nft utxo op error: %v", txn.Hash().String())
		}
		stateChangeMetadata, ok := utxoOp.StateChangeMetadata.(*lib.UpdateNFTStateChangeMetadata)
		if !ok {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing update nft state change metadata error: %v", txn.Hash().String())
		}

		postEntry := stateChangeMetadata.NFTPostEntry

		additionalDESORoyaltiesMap := stateChangeMetadata.AdditionalDESORoyaltiesMap
		additionalCoinRoyaltiesMap := stateChangeMetadata.AdditionalCoinRoyaltiesMap
		txnMeta.UpdateNFTTxindexMetadata = &lib.UpdateNFTTxindexMetadata{
			NFTPostHashHex: hex.EncodeToString(realTxMeta.NFTPostHash[:]),
			IsForSale:      realTxMeta.IsForSale,
		}
		txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
			PublicKeyBase58Check: lib.PkToString(postEntry.PosterPublicKey, params),
			Metadata:             "NFTCreatorPublicKeyBase58Check",
		})
		for pubKeyIter := range additionalDESORoyaltiesMap {
			pubKey := pubKeyIter
			txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
				PublicKeyBase58Check: pubKey,
				Metadata:             "AdditionalNFTRoyaltyToCreatorPublicKeyBase58Check",
			})
		}
		for pubKeyIter := range additionalCoinRoyaltiesMap {
			pubKey := pubKeyIter
			txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
				PublicKeyBase58Check: pubKey,
				Metadata:             "AdditionalNFTRoyaltyToCoinPublicKeyBase58Check",
			})
		}
	case lib.TxnTypeNFTTransfer:
		realTxMeta := txn.TxnMeta.(*lib.NFTTransferMetadata)

		txnMeta.NFTTransferTxindexMetadata = &lib.NFTTransferTxindexMetadata{
			NFTPostHashHex: hex.EncodeToString(realTxMeta.NFTPostHash[:]),
			SerialNumber:   realTxMeta.SerialNumber,
		}

		txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
			PublicKeyBase58Check: lib.PkToString(realTxMeta.ReceiverPublicKey, params),
			Metadata:             "NFTTransferRecipientPublicKeyBase58Check",
		})
	case lib.TxnTypeAcceptNFTTransfer:
		realTxMeta := txn.TxnMeta.(*lib.AcceptNFTTransferMetadata)

		txnMeta.AcceptNFTTransferTxindexMetadata = &lib.AcceptNFTTransferTxindexMetadata{
			NFTPostHashHex: hex.EncodeToString(realTxMeta.NFTPostHash[:]),
			SerialNumber:   realTxMeta.SerialNumber,
		}
	case lib.TxnTypeBurnNFT:
		realTxMeta := txn.TxnMeta.(*lib.BurnNFTMetadata)

		txnMeta.BurnNFTTxindexMetadata = &lib.BurnNFTTxindexMetadata{
			NFTPostHashHex: hex.EncodeToString(realTxMeta.NFTPostHash[:]),
			SerialNumber:   realTxMeta.SerialNumber,
		}
	case lib.TxnTypeBasicTransfer:
		// Add the public key of the receiver to the affected public keys.
		utxoOp := getUtxoOpByOperationType(utxoOps, lib.OperationTypeAddBalance)
		if utxoOp != nil {
			txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
				PublicKeyBase58Check: lib.PkToString(utxoOp.BalancePublicKey, params),
				Metadata:             "BasicTransferAddBalancePublicKeyBase58Check",
			})
		}
		diamondLevelBytes, hasDiamondLevel := txn.ExtraData[lib.DiamondLevelKey]
		diamondPostHash, hasDiamondPostHash := txn.ExtraData[lib.DiamondPostHashKey]
		if hasDiamondLevel && hasDiamondPostHash {
			diamondLevel, bytesRead := lib.Varint(diamondLevelBytes)
			if bytesRead <= 0 {
				glog.Errorf("Update TxIndex: Error reading diamond level for txn: %v", txn.Hash().String())
			} else {
				txnMeta.BasicTransferTxindexMetadata.DiamondLevel = diamondLevel
				txnMeta.BasicTransferTxindexMetadata.PostHashHex = hex.EncodeToString(diamondPostHash)
			}
		}
	case lib.TxnTypeDAOCoin:
		realTxMeta := txn.TxnMeta.(*lib.DAOCoinMetadata)

		utxoOp := getUtxoOpByOperationType(utxoOps, lib.OperationTypeDAOCoin)
		if utxoOp == nil || utxoOp.StateChangeMetadata == nil {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing dao coin utxo op error: %v", txn.Hash().String())
		}
		stateChangeMetadata, ok := utxoOp.StateChangeMetadata.(*lib.DAOCoinStateChangeMetadata)
		if !ok {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing dao coin state change metadata error: %v", txn.Hash().String())
		}

		creatorProfileEntry := stateChangeMetadata.CreatorProfileEntry

		var metadata string
		var operationString string
		switch realTxMeta.OperationType {
		case lib.DAOCoinOperationTypeMint:
			metadata = "DAOCoinMintPublicKeyBase58Check"
			operationString = "mint"
		case lib.DAOCoinOperationTypeBurn:
			metadata = "DAOCoinBurnPublicKeyBase58Check"
			operationString = "burn"
		case lib.DAOCoinOperationTypeDisableMinting:
			metadata = "DAOCoinDisableMintingPublicKeyBase58Check"
			operationString = "disable_minting"
		case lib.DAOCoinOperationTypeUpdateTransferRestrictionStatus:
			metadata = "DAOCoinUpdateTransferRestrictionStatus"
			operationString = "update_transfer_restriction_status"
		}

		txnMeta.DAOCoinTxindexMetadata = &lib.DAOCoinTxindexMetadata{
			CreatorUsername:           string(creatorProfileEntry.Username),
			OperationType:             operationString,
			CoinsToMintNanos:          &realTxMeta.CoinsToMintNanos,
			CoinsToBurnNanos:          &realTxMeta.CoinsToBurnNanos,
			TransferRestrictionStatus: realTxMeta.TransferRestrictionStatus.String(),
		}

		txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
			PublicKeyBase58Check: lib.PkToString(creatorProfileEntry.PublicKey, params),
			Metadata:             metadata,
		})
	case lib.TxnTypeDAOCoinTransfer:
		realTxMeta := txn.TxnMeta.(*lib.DAOCoinTransferMetadata)

		utxoOp := getUtxoOpByOperationType(utxoOps, lib.OperationTypeDAOCoinTransfer)
		if utxoOp == nil || utxoOp.StateChangeMetadata == nil {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing dao coin transfer utxo op error: %v", txn.Hash().String())
		}
		stateChangeMetadata, ok := utxoOp.StateChangeMetadata.(*lib.DAOCoinTransferStateChangeMetadata)
		if !ok {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing dao coin transfer state change metadata error: %v", txn.Hash().String())
		}

		creatorProfileEntry := stateChangeMetadata.CreatorProfileEntry
		txnMeta.DAOCoinTransferTxindexMetadata = &lib.DAOCoinTransferTxindexMetadata{
			CreatorUsername:        string(creatorProfileEntry.Username),
			DAOCoinToTransferNanos: realTxMeta.DAOCoinToTransferNanos,
		}

		txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
			PublicKeyBase58Check: lib.PkToString(realTxMeta.ReceiverPublicKey, params),
			Metadata:             "ReceiverPublicKey",
		})
	case lib.TxnTypeDAOCoinLimitOrder:
		realTxMeta := txn.TxnMeta.(*lib.DAOCoinLimitOrderMetadata)

		utxoOp := getUtxoOpByOperationType(utxoOps, lib.OperationTypeDAOCoinLimitOrder)
		if utxoOp == nil || utxoOp.StateChangeMetadata == nil {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing dao coin limit order utxo op error: %v", txn.Hash().String())
		}

		stateChangeMetadata, ok := utxoOp.StateChangeMetadata.(*lib.DAOCoinLimitOrderStateChangeMetadata)
		if !ok {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing dao coin limit order state change metadata error: %v", txn.Hash().String())
		}

		// We only update the mempool if the transactor submitted a new
		// order. Not if the transactor cancelled an existing order.
		if realTxMeta.CancelOrderID != nil {
			break
		}

		if !realTxMeta.BuyingDAOCoinCreatorPublicKey.IsZeroPublicKey() {
			txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
				PublicKeyBase58Check: lib.PkToString(realTxMeta.BuyingDAOCoinCreatorPublicKey.ToBytes(), params),
				Metadata:             "BuyingDAOCoinCreatorPublicKey",
			})
		}

		if !realTxMeta.SellingDAOCoinCreatorPublicKey.IsZeroPublicKey() {
			txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
				PublicKeyBase58Check: lib.PkToString(realTxMeta.SellingDAOCoinCreatorPublicKey.ToBytes(), params),
				Metadata:             "SellingDAOCoinCreatorPublicKey",
			})
		}

		uniquePublicKeyMap := make(map[string]bool)
		fulfilledOrderMetadata := []*lib.FilledDAOCoinLimitOrderMetadata{}
		for _, filledOrder := range stateChangeMetadata.FilledDAOCoinLimitOrdersMetadata {
			uniquePublicKeyMap[filledOrder.TransactorPublicKeyBase58Check] = true
			fulfilledOrderMetadata = append(fulfilledOrderMetadata, &lib.FilledDAOCoinLimitOrderMetadata{
				TransactorPublicKeyBase58Check: filledOrder.TransactorPublicKeyBase58Check,
				BuyingDAOCoinCreatorPublicKey:  filledOrder.BuyingDAOCoinCreatorPublicKey,
				SellingDAOCoinCreatorPublicKey: filledOrder.SellingDAOCoinCreatorPublicKey,
				CoinQuantityInBaseUnitsBought:  filledOrder.CoinQuantityInBaseUnitsBought,
				CoinQuantityInBaseUnitsSold:    filledOrder.CoinQuantityInBaseUnitsSold,
				IsFulfilled:                    filledOrder.IsFulfilled,
			})
		}

		for uniquePublicKey := range uniquePublicKeyMap {
			txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
				PublicKeyBase58Check: uniquePublicKey,
				Metadata:             "FilledOrderPublicKey",
			})
		}

		txnMeta.DAOCoinLimitOrderTxindexMetadata = &lib.DAOCoinLimitOrderTxindexMetadata{
			FilledDAOCoinLimitOrdersMetadata: fulfilledOrderMetadata,
			BuyingDAOCoinCreatorPublicKey: lib.PkToString(
				realTxMeta.BuyingDAOCoinCreatorPublicKey.ToBytes(), params),
			SellingDAOCoinCreatorPublicKey: lib.PkToString(
				realTxMeta.SellingDAOCoinCreatorPublicKey.ToBytes(), params),
			ScaledExchangeRateCoinsToSellPerCoinToBuy: realTxMeta.ScaledExchangeRateCoinsToSellPerCoinToBuy,
			QuantityToFillInBaseUnits:                 realTxMeta.QuantityToFillInBaseUnits,
		}

	case lib.TxnTypeCreateUserAssociation:
		realTxMeta := txn.TxnMeta.(*lib.CreateUserAssociationMetadata)
		targetUserPublicKeyBase58Check := lib.PkToString(realTxMeta.TargetUserPublicKey.ToBytes(), params)
		appPublicKeyBase58Check := lib.PkToString(realTxMeta.AppPublicKey.ToBytes(), params)

		txnMeta.CreateUserAssociationTxindexMetadata = &lib.CreateUserAssociationTxindexMetadata{
			TargetUserPublicKeyBase58Check: targetUserPublicKeyBase58Check,
			AppPublicKeyBase58Check:        appPublicKeyBase58Check,
			AssociationType:                string(realTxMeta.AssociationType),
			AssociationValue:               string(realTxMeta.AssociationValue),
		}

		txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
			PublicKeyBase58Check: targetUserPublicKeyBase58Check,
			Metadata:             "AssociationTargetUserPublicKeyBase58Check",
		})

	case lib.TxnTypeDeleteUserAssociation:
		realTxMeta := txn.TxnMeta.(*lib.DeleteUserAssociationMetadata)
		utxoOp := getUtxoOpByOperationType(utxoOps, lib.OperationTypeDeleteUserAssociation)
		if utxoOp == nil || utxoOp.StateChangeMetadata == nil {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing delete user association utxo op error: %v", txn.Hash().String())
		}
		stateChangeMetadata, ok := utxoOp.StateChangeMetadata.(*lib.DeleteUserAssociationStateChangeMetadata)
		if !ok {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing delete user association state change metadata error: %v", txn.Hash().String())
		}

		prevAssociationEntry := &lib.UserAssociationEntry{}
		targetUserPublicKeyBase58Check := ""
		appPublicKeyKeyBase58Check := ""
		if utxoOps[len(utxoOps)-1].PrevUserAssociationEntry != nil {
			prevAssociationEntry = utxoOps[len(utxoOps)-1].PrevUserAssociationEntry
			targetUserPublicKeyBase58Check = stateChangeMetadata.TargetUserPublicKeyBase58Check
			appPublicKeyKeyBase58Check = stateChangeMetadata.AppPublicKeyBase58Check
		}

		txnMeta.DeleteUserAssociationTxindexMetadata = &lib.DeleteUserAssociationTxindexMetadata{
			AssociationIDHex:               hex.EncodeToString(realTxMeta.AssociationID.ToBytes()),
			TargetUserPublicKeyBase58Check: targetUserPublicKeyBase58Check,
			AppPublicKeyBase58Check:        appPublicKeyKeyBase58Check,
			AssociationType:                string(prevAssociationEntry.AssociationType),
			AssociationValue:               string(prevAssociationEntry.AssociationValue),
		}

		txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
			PublicKeyBase58Check: targetUserPublicKeyBase58Check,
			Metadata:             "AssociationTargetUserPublicKeyBase58Check",
		})

	case lib.TxnTypeCreatePostAssociation:
		realTxMeta := txn.TxnMeta.(*lib.CreatePostAssociationMetadata)
		utxoOp := getUtxoOpByOperationType(utxoOps, lib.OperationTypeCreatePostAssociation)
		if utxoOp == nil || utxoOp.StateChangeMetadata == nil {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing create post association utxo op error: %v", txn.Hash().String())
		}
		stateChangeMetadata, ok := utxoOp.StateChangeMetadata.(*lib.CreatePostAssociationStateChangeMetadata)
		if !ok {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing create post association state change metadata error: %v", txn.Hash().String())
		}

		appPublicKeyBase58Check := lib.PkToString(realTxMeta.AppPublicKey.ToBytes(), params)

		txnMeta.CreatePostAssociationTxindexMetadata = &lib.CreatePostAssociationTxindexMetadata{
			PostHashHex:             hex.EncodeToString(realTxMeta.PostHash.ToBytes()),
			AppPublicKeyBase58Check: appPublicKeyBase58Check,
			AssociationType:         string(realTxMeta.AssociationType),
			AssociationValue:        string(realTxMeta.AssociationValue),
		}

		postEntry := stateChangeMetadata.PostEntry

		txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
			PublicKeyBase58Check: lib.PkToString(postEntry.PosterPublicKey, params),
			Metadata:             "AssociationTargetPostCreatorPublicKeyBase58Check",
		})

	case lib.TxnTypeDeletePostAssociation:
		realTxMeta := txn.TxnMeta.(*lib.DeletePostAssociationMetadata)
		utxoOp := getUtxoOpByOperationType(utxoOps, lib.OperationTypeDeletePostAssociation)
		if utxoOp == nil || utxoOp.StateChangeMetadata == nil {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing delete post association utxo op error: %v", txn.Hash().String())
		}
		stateChangeMetadata, ok := utxoOp.StateChangeMetadata.(*lib.DeletePostAssociationStateChangeMetadata)
		if !ok {
			return nil, fmt.Errorf("ComputeTransactionMetadata: missing delete post association state change metadata error: %v", txn.Hash().String())
		}

		prevAssociationEntry := &lib.PostAssociationEntry{}
		postHashHex := ""
		appPublicKeyKeyBase58Check := ""
		postAuthorPublicKeyBase58Check := ""
		if utxoOps[len(utxoOps)-1].PrevPostAssociationEntry != nil {
			prevAssociationEntry = utxoOps[len(utxoOps)-1].PrevPostAssociationEntry
			postHashHex = hex.EncodeToString(prevAssociationEntry.PostHash.ToBytes())
			appPublicKeyKeyBase58Check = stateChangeMetadata.AppPublicKeyBase58Check
			postAuthorPublicKeyBase58Check = lib.PkToString(stateChangeMetadata.PostEntry.PosterPublicKey, params)
		}

		txnMeta.DeletePostAssociationTxindexMetadata = &lib.DeletePostAssociationTxindexMetadata{
			AssociationIDHex:        hex.EncodeToString(realTxMeta.AssociationID.ToBytes()),
			PostHashHex:             postHashHex,
			AppPublicKeyBase58Check: appPublicKeyKeyBase58Check,
			AssociationType:         string(prevAssociationEntry.AssociationType),
			AssociationValue:        string(prevAssociationEntry.AssociationValue),
		}

		txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
			PublicKeyBase58Check: postAuthorPublicKeyBase58Check,
			Metadata:             "AssociationTargetPostCreatorPublicKeyBase58Check",
		})

	case lib.TxnTypeAccessGroup:
		realTxMeta := txn.TxnMeta.(*lib.AccessGroupMetadata)
		txnMeta.AccessGroupTxindexMetadata = &lib.AccessGroupTxindexMetadata{
			AccessGroupOwnerPublicKey: *lib.NewPublicKey(realTxMeta.AccessGroupOwnerPublicKey),
			AccessGroupPublicKey:      *lib.NewPublicKey(realTxMeta.AccessGroupPublicKey),
			AccessGroupKeyName:        *lib.NewGroupKeyName(realTxMeta.AccessGroupKeyName),
			AccessGroupOperationType:  realTxMeta.AccessGroupOperationType,
		}

		txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
			PublicKeyBase58Check: lib.PkToString(realTxMeta.AccessGroupOwnerPublicKey, params),
			Metadata:             "AccessGroupCreateOwnerPublicKeyBase58Check",
		})

	case lib.TxnTypeAccessGroupMembers:
		realTxMeta := txn.TxnMeta.(*lib.AccessGroupMembersMetadata)
		txnMeta.AccessGroupMembersTxindexMetadata = &lib.AccessGroupMembersTxindexMetadata{
			AccessGroupOwnerPublicKey:      *lib.NewPublicKey(realTxMeta.AccessGroupOwnerPublicKey),
			AccessGroupKeyName:             *lib.NewGroupKeyName(realTxMeta.AccessGroupKeyName),
			AccessGroupMembersList:         realTxMeta.AccessGroupMembersList,
			AccessGroupMemberOperationType: realTxMeta.AccessGroupMemberOperationType,
		}

		txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
			PublicKeyBase58Check: lib.PkToString(realTxMeta.AccessGroupOwnerPublicKey, params),
			Metadata:             "AccessGroupMembersOwnerPublicKeyBase58Check",
		})

	case lib.TxnTypeNewMessage:
		realTxMeta := txn.TxnMeta.(*lib.NewMessageMetadata)
		txnMeta.NewMessageTxindexMetadata = &lib.NewMessageTxindexMetadata{
			SenderAccessGroupOwnerPublicKey:    realTxMeta.SenderAccessGroupOwnerPublicKey,
			SenderAccessGroupKeyName:           realTxMeta.SenderAccessGroupKeyName,
			RecipientAccessGroupOwnerPublicKey: realTxMeta.RecipientAccessGroupOwnerPublicKey,
			RecipientAccessGroupKeyName:        realTxMeta.RecipientAccessGroupKeyName,
			TimestampNanos:                     realTxMeta.TimestampNanos,
			NewMessageType:                     realTxMeta.NewMessageType,
			NewMessageOperation:                realTxMeta.NewMessageOperation,
		}

		txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
			PublicKeyBase58Check: lib.PkToString(realTxMeta.SenderAccessGroupOwnerPublicKey.ToBytes(), params),
			Metadata:             "NewMessageSenderAccessGroupOwnerPublicKey",
		})
		txnMeta.AffectedPublicKeys = append(txnMeta.AffectedPublicKeys, &lib.AffectedPublicKey{
			PublicKeyBase58Check: lib.PkToString(realTxMeta.RecipientAccessGroupOwnerPublicKey.ToBytes(), params),
			Metadata:             "NewMessageRecipientAccessGroupOwnerPublicKe",
		})
	}
	return txnMeta, nil
}

func getUtxoOpByOperationType(utxoOps []*lib.UtxoOperation, operationType lib.OperationType) *lib.UtxoOperation {
	for _, utxoOp := range utxoOps {
		if utxoOp.Type == operationType {
			return utxoOp
		}
	}
	return nil
}

// CheckSliceSize checks if the requested slice size is within safe limits.
func CheckSliceSize(length int) error {
	const maxInt = int(^uint(0) >> 1) // platform-dependent maximum int value

	if length < 0 {
		return errors.New("length or capacity cannot be negative")
	}
	if length > maxInt {
		return errors.New("requested slice size exceeds maximum allowed size")
	}
	return nil
}
