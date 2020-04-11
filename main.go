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
	DateTime time.Time `json:"dateTime" binding:"required" db:"datetime"`
	CentsAmount int64 `json:"centsAmount" binding:"required" db:"cents_amount"`
	CurrencyCode string `json:"currencyCode" binding:"required" db:"currency_code"`
	Reference string `json:"reference" binding:"required" db:"reference"`
	MerchantName string `json:"merchantName" binding:"required" db:"merchant_name"`
	MerchantCity string `json:"merchantCity" binding:"required" db:"merchant_city"`
	MerchantCountryCode string `json:"merchantCountryCode" binding:"required" db:"merchant_country_code"`
	MerchantCountryName string `json:"merchantCountryName" binding:"required" db:"merchant_country_name"`
	MerchantCategoryCode string `json:"merchantCategoryCode" binding:"required" db:"merchant_category_code"`
	MerchantCategoryName string `json:"merchantCategoryName" binding:"required" db:"merchant_category_name"`
}


func postTransactionHandler(db *sqlx.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS transactions (id SERIAL, datetime timestamptz, 
										cents_amount NUMERIC (16, 2), currency_code VARCHAR NOT NULL, 
										reference VARCHAR NOT NULL, merchant_name VARCHAR NOT NULL,
										merchant_city VARCHAR NOT NULL, merchant_country_code VARCHAR NOT NULL,
										merchant_country_name VARCHAR NOT NULL, merchant_category_code VARCHAR NOT NULL, 
										merchant_category_name VARCHAR NOT NULL)`); err != nil {
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
				(date_time, cents_amount, currency_code, reference, merchant_name,
				merchant_city, merchant_country_code, merchant_country_name, 
				merchant_category_code, merchant_category_name)
				VALUES (:date_time, :cents_amount, :currency_code, :reference, :merchant_name,
				:merchant_city, :merchant_country_code, :merchant_country_name, 
				:merchant_category_code, :merchant_category_name)`
		tx := db.MustBegin()
		_, err := tx.NamedExec(insertStatement, transaction)
		if err != nil {
			c.String(http.StatusInternalServerError,
				fmt.Sprintf("Error inserting transaction: %q", err))
			return
		}
		tx.Commit()
	}
}

func getTransactionHandler(db *sqlx.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		var transactions []Transaction
		queryStatement := `SELECT date_time, cents_amount, currency_code, reference, merchant_name,
		merchant_city, merchant_country_code, merchant_country_name,
			merchant_category_code, merchant_category_name FROM transactions`
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