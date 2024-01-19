package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sort"
	"strings"

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

var startFlag = flag.Bool("start", false, "Start the database creation process")

func main() {
	flag.Parse()

	if *startFlag {
		startDatabaseCreation()
		return
	} else {

		log.Println("Sagep restore started!")
		c := cron.New()

		_, err := c.AddFunc("0 22 * * *", func() {
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
				log.Println(errors.New("no files found in the specified folder"))
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
			sqlStatements := `
			DROP SCHEMA IF EXISTS public CASCADE;
			CREATE SCHEMA public;
			COMMENT ON SCHEMA public
			IS 'standard public schema';
			GRANT ALL ON SCHEMA public TO PUBLIC;
			GRANT ALL ON SCHEMA public TO pg_database_owner;
			GRANT ALL ON SCHEMA public TO postgres;
			`

			psqlCommandRecreateSchema := exec.Command(PSQL_PATH, "-h", DB_HOST, "-U", DB_USER, "-d", DB_NAME)
			psqlCommandRecreateSchema.Env = append(os.Environ(), "PGPASSWORD="+DB_PASSWORD)
			psqlCommandRecreateSchema.Stdin = strings.NewReader(sqlStatements)

			output, err := psqlCommandRecreateSchema.CombinedOutput()
			if err != nil {
				log.Printf("Error running psql: %v\n", err)
				log.Printf("Command output: %s\n", output)
				return
			}
			log.Println("Public schema dropped and recreated successfully!")

			psqlCommandRestore := exec.Command(PSQL_PATH, "-h", DB_HOST, "-U", DB_USER, "-d", DB_NAME)
			psqlCommandRestore.Env = append(os.Environ(), "PGPASSWORD="+DB_PASSWORD)
			psqlCommandRestore.Stdin = resultBackup.Body

			log.Println("Running pqsql restore command...")

			output, err = psqlCommandRestore.CombinedOutput()
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
	}

	select {}
}

func startDatabaseCreation() {
	log.Println("Starting building now")
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
		log.Println(errors.New("no files found in the specified folder"))
		return
	}

	sort.Slice(result.Contents, func(i, j int) bool {
		return result.Contents[i].LastModified.After(*result.Contents[j].LastModified)
	})

	lastObject := result.Contents[0]

	resultBackup, err := s3Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(BUCKET),
		Key:    aws.String(*lastObject.Key),
	})
	if err != nil {
		log.Println(err)
		return
	}

	if err := checkDatabaseExists(DB_NAME); err != nil {
		log.Println(err)
		return
	}
	log.Printf("Restoring database '%s'...\n", DB_NAME)
	log.Printf("Backup file: %s, Size: %d bytes\n", *lastObject.Key, *lastObject.Size)
	psqlCommandRestore := exec.Command(PSQL_PATH, "-h", DB_HOST, "-U", DB_USER, "-d", DB_NAME)
	psqlCommandRestore.Env = append(os.Environ(), "PGPASSWORD="+DB_PASSWORD)
	psqlCommandRestore.Stdin = resultBackup.Body

	output, err := psqlCommandRestore.CombinedOutput()
	if err != nil {
		log.Printf("Error running psql: %v\n", err)
		log.Printf("Command output: %s\n", output)
		return
	}
	log.Printf("Database '%s' created and restored successfully!\n", DB_NAME)
}

func checkDatabaseExists(dbName string) error {
	psqlCommandCheckDB := exec.Command(PSQL_PATH, "-h", DB_HOST, "-U", DB_USER, "-lqt")
	psqlCommandCheckDB.Env = append(os.Environ(), "PGPASSWORD="+DB_PASSWORD)

	output, err := psqlCommandCheckDB.CombinedOutput()
	if err != nil {
		log.Fatalf("error checking if the database exists: %v\nCommand output: %s\n", err, output)
	}

	if !strings.Contains(string(output), dbName) {
		psqlCommandCreateDB := exec.Command(PSQL_PATH, "-h", DB_HOST, "-U", DB_USER, "-c", fmt.Sprintf(`CREATE DATABASE "%s"`, dbName))
		psqlCommandCreateDB.Env = append(os.Environ(), "PGPASSWORD="+DB_PASSWORD)

		output, err := psqlCommandCreateDB.CombinedOutput()
		if err != nil {
			log.Fatalf("error creating the database: %v\nCommand output: %s\n", err, output)
		}

		log.Printf("Database '%s' created successfully!\n", dbName)
	}

	return nil
}
