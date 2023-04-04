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

type testResponse struct {
	PosterPublicKey string `decode_function:"base_58_check" decode_src_field_name:"PosterPublicKey"`
	PostHash        string `decode_function:"blockhash" decode_src_field_name:"PostHash"`
	ParentPostHash  string `decode_function:"bytehash" decode_src_field_name:"ParentStakeID"`
	Body            string `decode_function:"deso_body_schema" decode_src_field_name:"Body" decode_body_field_name:"Body" decode_image_urls_field_name:"ImageUrls" decode_video_urls_field_name:"VideoUrls"`
	ImageUrls       []string
	VideoUrls       []string
	Timestamp       time.Time `decode_function:"timestamp" decode_src_field_name:"TimestampNanos"`
}

func TestCopyStruct(t *testing.T) {
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

	struct1 := &lib.PostEntry{
		TimestampNanos:  uint64(time.Now().UnixNano()),
		PostHash:        blockHash,
		ParentStakeID:   blockHash.ToBytes(),
		Body:            bodyBytes,
		PosterPublicKey: []byte{2, 57, 123, 26, 128, 235, 160, 166, 6, 68, 101, 10, 241, 60, 42, 111, 253, 251, 191, 56, 131, 12, 175, 195, 73, 55, 167, 93, 221, 68, 184, 206, 82},
	}

	struct2 := &testResponse{}

	err = CopyStruct(struct1, struct2)

	require.NoError(t, err)
	//require.Equal(t, )
	fmt.Printf("struct2: %+v\n", struct2)
}
