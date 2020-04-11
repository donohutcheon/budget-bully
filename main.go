package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/heroku/x/hmetrics/onload"
	_ "github.com/lib/pq"
	"github.com/jmoiron/sqlx"
)

type Transaction struct {
	DateTime time.Time `json:"dateTime" binding:"required" db:"dateTime"`
	CentsAmount int64 `json:"centsAmount" binding:"required" db:"centsAmount"`
	CurrencyCode string `json:"currencyCode" binding:"required" db:"currencyCode"`
	Reference string `json:"reference" binding:"required" db:"reference"`
	MerchantName string `json:"merchantName" binding:"required" db:"merchantName"`
	MerchantCity string `json:"merchantCity" binding:"required" db:"merchantCity"`
	MerchantCountryCode string `json:"merchantCountryCode" binding:"required" db:"merchantCountryCode"`
	MerchantCountryName string `json:"merchantCountryName" binding:"required" db:"merchantCountryName"`
	MerchantCategoryCode string `json:"merchantCategoryCode" binding:"required" db:"merchantCategoryCode"`
	MerchantCategoryName string `json:"merchantCategoryName" binding:"required" db:"merchantCategoryName"`
}


func postTransactionHandler(db *sqlx.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS transactions (id SERIAL, dateTime timestamptz, 
										centsAmount NUMERIC (16, 2), currencyCode VARCHAR NOT NULL, 
										reference VARCHAR NOT NULL, merchantName VARCHAR NOT NULL,
										merchantCity VARCHAR NOT NULL, merchantCountryCode VARCHAR NOT NULL,
										merchantCountryName VARCHAR NOT NULL, merchantCategoryCode VARCHAR NOT NULL, 
										merchantCategoryName VARCHAR NOT NULL)`); err != nil {
			c.String(http.StatusInternalServerError,
				fmt.Sprintf("Error creating database table: %q", err))
			return
		}

		var transaction Transaction
		if err := c.ShouldBindJSON(&transaction); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		insertStatement := `INSERT INTO transactions
				(dateTime, centsAmount, currencyCode, reference, merchantName,
				merchantCity, merchantCountryCode, merchantCategoryName, 
				merchantCategoryCode, merchantCategoryName)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`
		tx := db.MustBegin()
		_, err := tx.NamedExec(insertStatement, transaction)
		if err != nil {
			c.String(http.StatusInternalServerError,
				fmt.Sprintf("Error incrementing tick: %q", err))
			return
		}
		tx.Commit()
	}
}

func getTransactionHandler(db *sqlx.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		var transactions []Transaction
		queryStatement := `SELECT dateTime, centsAmount, currencyCode, reference, merchantName,
		merchantCity, merchantCountryCode, merchantCategoryName,
			merchantCategoryCode, merchantCategoryName FROM transactions`
		rows, err := db.Queryx(queryStatement)
		if err != nil {
			c.String(http.StatusInternalServerError,
				fmt.Sprintf("Error reading transactions: %q", err))
			return
		}

		defer rows.Close()
		for rows.Next() {
			var transaction Transaction
			err := rows.StructScan(&transaction)
			if err != nil {
				c.String(http.StatusInternalServerError,
					fmt.Sprintf("Error scanning transactions: %q", err))
				return
			}

			transactions = append(transactions, transaction)
		}
		var bytes []byte
		err = json.Unmarshal(bytes, transactions)
		if err != nil {
			c.String(http.StatusInternalServerError,
				fmt.Sprintf("Error unmarshalling transaction: %q", err))
			return
		}

		c.String(http.StatusOK, fmt.Sprintf("Read from DB: %s\n", string(bytes)))
	}
}

func main() {
	port := os.Getenv("PORT")

	if port == "" {
		log.Fatal("$PORT must be set")
	}

	db, err := sqlx.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("Error opening database: %q", err)
	}

	router := gin.New()
	router.Use(gin.Logger())

	router.GET("/", func(c *gin.Context) {
		c.String(http.StatusOK, string("hi!"))
	})

	router.POST("/transaction", postTransactionHandler(db))
	router.GET("/transaction", getTransactionHandler(db))

	router.Run(":" + port)
}