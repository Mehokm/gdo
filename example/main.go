package main

import (
	"database/sql"
	"fmt"
	"gdo"
	"log"
	"math/rand"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	db, err := sql.Open("mysql", "cimysql:cimysql-password@tcp(localhost)/local_test")

	if err != nil {
		log.Fatal(err)
	}

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	doSelect(db)
	doInsert(db)
	doUpdate(db)

	doInsertTx(db)
}

func doSelect(db *sql.DB) {
	g := gdo.New(db)

	stmt := gdo.NewStatement("SELECT * FROM Test WHERE `IntCol` <> @intCol AND `StringCol` = @strCol AND `StringCol` <> @intCol")

	stmt.BindParams([]sql.NamedArg{
		sql.Named("intCol", 11),
		sql.Named("strCol", "good bye"),
	})

	r, err := g.Query(stmt)

	if err != nil {
		log.Println(err)
	}

	m := r.FetchMap()

	fmt.Println(r.LastExecutedQuery())
	fmt.Println(m)
}

func doInsert(db *sql.DB) {
	g := gdo.New(db)

	stmt := gdo.NewStatement("INSERT INTO Test (IntCol, StringCol) VALUES (@intCol, @strCol)")

	stmt.BindParams([]sql.NamedArg{
		sql.Named("intCol", 11),
		sql.Named("strCol", randomString()),
	})

	r, err := g.Exec(stmt)

	if err != nil {
		log.Println(err)
	}

	id, err := r.LastInsertId()

	if err != nil {
		log.Println(err)
	}

	fmt.Println(r.LastExecutedQuery())
	fmt.Println(id)
}

func doInsertTx(db *sql.DB) {
	g := gdo.New(db)

	tx, err := g.Begin()

	stmt := gdo.NewStatement("INSERT INTO Test (IntCol, StringCol) VALUES (@intCol, @strCol)")

	stmt.BindParams([]sql.NamedArg{
		sql.Named("intCol", 1101),
		sql.Named("strCol", randomString()),
	})

	r, err := tx.Exec(stmt)

	if err != nil {
		log.Println(err)
	}

	id, err := r.LastInsertId()

	if err != nil {
		log.Println(err)
	}

	err = tx.Commit()

	if err != nil {
		log.Println(err)
	}

	fmt.Println(r.LastExecutedQuery())
	fmt.Println(id)
}

func doUpdate(db *sql.DB) {
	g := gdo.New(db)

	stmt := gdo.NewStatement("UPDATE Test SET `IntCol`=@intCol WHERE `id`=@id")
	stmt.BindParams([]sql.NamedArg{
		sql.Named("intCol", 10.10),
		sql.Named("id", 1),
	})

	r, err := g.Exec(stmt)

	if err != nil {
		log.Println(err)
	}

	rows, err := r.RowsAffected()

	if err != nil {
		log.Println(err)
	}

	fmt.Println(r.LastExecutedQuery())
	fmt.Println(rows)
}

func randomString() string {
	N := 10
	low := 65
	high := 122

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	b := make([]byte, N)

	for i := 0; i < N; i++ {
		b[i] = byte(r.Intn(high-low) + low)
	}

	return string(b)
}
