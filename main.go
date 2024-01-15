package main

import (
	"errors"
	"log"
	"os"
	"os/exec"
	"sort"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/joho/godotenv"
	"github.com/robfig/cron/v3"
)

var REGION string
var BUCKET string
var AWS_ACCESS_KEY string
var AWS_SECRET_KEY string
var DB_USER string
var DB_HOST string
var DB_NAME string
var DB_PASSWORD string
var PSQL_PATH string

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Println(err)
	}
	REGION = os.Getenv("AWS_REGION")
	BUCKET = os.Getenv("AWS_BUCKET")
	AWS_ACCESS_KEY = os.Getenv("AWS_ACCESS_KEY")
	AWS_SECRET_KEY = os.Getenv("AWS_SECRET_KEY")

	DB_USER = os.Getenv("DB_USER")
	DB_HOST = os.Getenv("DB_HOST")
	DB_NAME = os.Getenv("DB_NAME")
	DB_PASSWORD = os.Getenv("DB_PASSWORD")

	PSQL_PATH = os.Getenv("PSQL_PATH")
}

func main() {
	log.Println("Restore started!")
	c := cron.New()

	_, err := c.AddFunc("0 22 * * *", func() { // Restore every day at 22:00 pm
		log.Println("Starting restore now")
		keyFolder := ""

		sess, err := session.NewSession(&aws.Config{
			Region:      aws.String(REGION),
			Credentials: credentials.NewStaticCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY, ""),
		})
		if err != nil {
			log.Println(err)
			return
		}

		s3Client := s3.New(sess)

		params := &s3.ListObjectsV2Input{
			Bucket: aws.String(BUCKET),
			Prefix: aws.String(keyFolder),
		}

		result, err := s3Client.ListObjectsV2(params)
		if err != nil {
			log.Println(err)
			return
		}

		if len(result.Contents) <= 0 {
			log.Println(errors.New("No files found in the specified folder"))
			return
		}

		sort.Slice(result.Contents, func(i, j int) bool {
			return result.Contents[i].LastModified.After(*result.Contents[j].LastModified)
		})

		lastObject := result.Contents[0]

		log.Printf("Backup file: %s, Size: %d bytes\n", *lastObject.Key, *lastObject.Size)
		resultBackup, err := s3Client.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(BUCKET),
			Key:    aws.String(*lastObject.Key),
		})
		if err != nil {
			log.Println(err)
			return
		}

		log.Printf("Backup file %s successfully retrieved\n", *lastObject.Key)

		restoreCommand := exec.Command(PSQL_PATH, "-h", DB_HOST, "-U", DB_USER, "-d", DB_NAME)

		restoreCommand.Env = append(os.Environ(), "PGPASSWORD="+DB_PASSWORD)

		restoreCommand.Stdin = resultBackup.Body

		log.Println("Running pqsql command...")

		output, err := restoreCommand.CombinedOutput()
		if err != nil {
			log.Printf("Error running pg_restore: %v\n", err)
			log.Printf("Command output: %s\n", output)
			return
		}

		log.Println("Database restore successful!")
	})
	if err != nil {
		log.Fatal(err)
	}

	c.Start()

	select {}
}
