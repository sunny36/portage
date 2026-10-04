// Command quickstart prepares the local emulators for examples/pipeline.yaml:
// it creates the Azurite container "source" and the SeaweedFS bucket "dest",
// then uploads a few sample files under incoming/.
//
//	docker compose -f deploy/docker-compose.yml up -d --wait
//	go run ./examples/quickstart
//	go run ./cmd/portage run -c examples/pipeline.yaml
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/sunny36/portage/internal/testenv"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	az, err := azblob.NewClientFromConnectionString(testenv.AzuriteConnectionString(), nil)
	if err != nil {
		return err
	}
	if _, err := az.CreateContainer(ctx, "source", nil); err != nil && !isCode(err, "ContainerAlreadyExists") {
		return fmt.Errorf("create container (is docker compose up?): %w", err)
	}

	ac, err := awscfg.LoadDefaultConfig(ctx, awscfg.WithRegion(testenv.S3Region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(testenv.S3AccessKeyID, testenv.S3SecretAccessKey, "")))
	if err != nil {
		return err
	}
	s3c := s3sdk.NewFromConfig(ac, func(o *s3sdk.Options) {
		o.BaseEndpoint = aws.String(testenv.S3Endpoint())
		o.UsePathStyle = true
	})
	if _, err := s3c.CreateBucket(ctx, &s3sdk.CreateBucketInput{Bucket: aws.String("dest")}); err != nil &&
		!isCode(err, "BucketAlreadyOwnedByYou") && !isCode(err, "BucketAlreadyExists") {
		return fmt.Errorf("create bucket: %w", err)
	}

	files := map[string][]byte{
		"incoming/hello.txt":        []byte("hello from Azure\n"),
		"incoming/reports/2026.csv": []byte("month,total\n1,100\n2,140\n"),
		"incoming/images/large.bin": bytes.Repeat([]byte("portage "), 3<<20), // 24 MiB: multipart
		"not-synced/outside-prefix": []byte("the pipeline only syncs incoming/\n"),
	}
	for name, b := range files {
		if _, err := az.UploadBuffer(ctx, "source", name, b, nil); err != nil {
			return fmt.Errorf("upload %s: %w", name, err)
		}
		fmt.Fprintf(os.Stdout, "uploaded azure://source/%s (%d bytes)\n", name, len(b))
	}
	fmt.Println("\nReady. Now run:\n  go run ./cmd/portage run -c examples/pipeline.yaml")
	return nil
}

func isCode(err error, code string) bool {
	var re *azcore.ResponseError
	if errors.As(err, &re) && re.ErrorCode == code {
		return true
	}
	var ae smithy.APIError
	return errors.As(err, &ae) && ae.ErrorCode() == code
}
