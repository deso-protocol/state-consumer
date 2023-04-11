package consumer

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/deso-protocol/core/lib"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type testPostResponse struct {
	PosterPublicKey string `decode_function:"base_58_check" decode_src_field_name:"PosterPublicKey"`
	PostHash        string `decode_function:"blockhash" decode_src_field_name:"PostHash"`
	ParentPostHash  string `decode_function:"bytehash" decode_src_field_name:"ParentStakeID"`
	Body            string `decode_function:"deso_body_schema" decode_src_field_name:"Body" decode_body_field_name:"Body" decode_image_urls_field_name:"ImageUrls" decode_video_urls_field_name:"VideoUrls"`
	ImageUrls       []string
	VideoUrls       []string
	Timestamp       time.Time `decode_function:"timestamp" decode_src_field_name:"TimestampNanos"`
}

type testProfileResponse struct {
	Username                         string                        `decode_function:"string_bytes" decode_src_field_name:"Username"`
	Description                      string                        `decode_function:"string_bytes" decode_src_field_name:"Description"`
	ExtraData                        map[string]string             `decode_function:"extra_data" decode_src_field_name:"ExtraData"`
	DaoCoinMintingDisabled           bool                          `decode_function:"nested_value" decode_src_field_name:"DAOCoinEntry" nested_field_name:"MintingDisabled"`
	DaoCoinTransferRestrictionStatus lib.TransferRestrictionStatus `decode_function:"nested_value" decode_src_field_name:"DAOCoinEntry" nested_field_name:"TransferRestrictionStatus"`
}

type testFollowResponse struct {
	FollowerPkid []byte `pg:",use_zero" decode_function:"pkid" decode_src_field_name:"FollowerPKID"`
	FollowedPkid []byte `pg:",use_zero" decode_function:"pkid" decode_src_field_name:"FollowedPKID"`
}

func TestCopyFollowStruct(t *testing.T) {
	followEntry := &lib.FollowEntry{
		FollowerPKID: lib.NewPKID([]byte{2, 57, 123, 26, 128, 235, 160, 166, 6, 68, 101, 10, 241, 60, 42, 111, 253, 251, 191, 56, 131, 12, 175, 195, 73, 55, 167, 93, 221, 68, 184, 206, 82}),
		FollowedPKID: lib.NewPKID([]byte{2, 57, 123, 26, 128, 235, 160, 166, 6, 68, 101, 10, 241, 60, 42, 111, 253, 251, 191, 56, 131, 12, 175, 195, 73, 55, 167, 93, 221, 68, 184, 206, 82}),
	}
	responseStruct := &testFollowResponse{}
	err := CopyStruct(followEntry, responseStruct)
	require.NoError(t, err)
	fmt.Printf("Response: %+v", responseStruct)
}

func TestCopyProfileStruct(t *testing.T) {
	usernameBytes := []byte("test_username")
	descriptionBytes := []byte("test_description")
	extraData := map[string][]byte{"test_key": []byte("test_value")}
	profileEntry := &lib.ProfileEntry{
		Username:    usernameBytes,
		Description: descriptionBytes,
		ExtraData:   extraData,
		DAOCoinEntry: lib.CoinEntry{
			MintingDisabled:           true,
			TransferRestrictionStatus: lib.TransferRestrictionStatusUnrestricted,
		},
	}
	responseStruct := &testProfileResponse{}
	err := CopyStruct(profileEntry, responseStruct)
	require.NoError(t, err)
	require.Equal(t, "test_username", responseStruct.Username)
	require.Equal(t, "test_description", responseStruct.Description)
	require.Equal(t, "test_value", responseStruct.ExtraData["test_key"])
	require.Equal(t, true, responseStruct.DaoCoinMintingDisabled)
	require.Equal(t, lib.TransferRestrictionStatusUnrestricted, responseStruct.DaoCoinTransferRestrictionStatus)

	profileEntry.Description = []byte{}

	err = CopyStruct(profileEntry, responseStruct)
	require.NoError(t, err)
	require.Equal(t, "test_username", responseStruct.Username)
	require.Equal(t, "", responseStruct.Description)
	require.Equal(t, "test_value", responseStruct.ExtraData["test_key"])
}

func TestCopyPostStruct(t *testing.T) {
	postBytesHex := "13a546bba07e9cd96e29cea659b3bb6de1b5144a50bf2a0c94d05701861d8254"
	byteArray, err := hex.DecodeString(postBytesHex)
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	blockHash := lib.NewBlockHash(byteArray)

	blockHash.ToBytes()
	postBody := &lib.DeSoBodySchema{
		Body:      "Test string",
		ImageURLs: []string{"https://test.com/image1.jpg", "https://test.com/image2.jpg"},
		VideoURLs: []string{"https://test.com/video1.mp4", "https://test.com/video2.mp4"},
	}

	bodyBytes, err := json.Marshal(postBody)

	currentTimeNanos := time.Now()

	struct1 := &lib.PostEntry{
		TimestampNanos:  uint64(currentTimeNanos.UnixNano()),
		PostHash:        blockHash,
		ParentStakeID:   blockHash.ToBytes(),
		Body:            bodyBytes,
		PosterPublicKey: []byte{2, 57, 123, 26, 128, 235, 160, 166, 6, 68, 101, 10, 241, 60, 42, 111, 253, 251, 191, 56, 131, 12, 175, 195, 73, 55, 167, 93, 221, 68, 184, 206, 82},
	}

	struct2 := &testPostResponse{}

	err = CopyStruct(struct1, struct2)

	require.NoError(t, err)
	fmt.Printf("struct2: %+v\n", struct2)
	require.Equal(t, currentTimeNanos.UnixNano(), struct2.Timestamp.UnixNano())
	struct2.Timestamp = time.Time{}
	require.Equal(t, &testPostResponse{
		PosterPublicKey: "BC1YLg7Bk5sq9iNY17bAwoAYiChLYpmWEi6nY6q5gnA1UQV6xixHjfV",
		PostHash:        "13a546bba07e9cd96e29cea659b3bb6de1b5144a50bf2a0c94d05701861d8254",
		ParentPostHash:  "13a546bba07e9cd96e29cea659b3bb6de1b5144a50bf2a0c94d05701861d8254",
		Body:            "Test string",
		ImageUrls:       []string{"https://test.com/image1.jpg", "https://test.com/image2.jpg"},
		VideoUrls:       []string{"https://test.com/video1.mp4", "https://test.com/video2.mp4"},
		Timestamp:       time.Time{},
	}, struct2)
}

func TestGetDisconnectOperationTypeForPrevEntry(t *testing.T) {
	prevPostEntry := &lib.PostEntry{}
	prevPostEntry = nil
	operationType := getDisconnectOperationTypeForPrevEntry(prevPostEntry)
	require.Equal(t, lib.DbOperationTypeDelete, operationType)
	prevPostEntry = &lib.PostEntry{}
	operationType = getDisconnectOperationTypeForPrevEntry(prevPostEntry)
	require.Equal(t, lib.DbOperationTypeUpsert, operationType)
}
