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
}

func doSelect(db *sql.DB) {
	g := gdo.New(db)

	stmt := gdo.Statement("SELECT * FROM Test WHERE `IntCol` = @intCol AND `StringCol`=@strCol AND `StringCol` <> @intCol")
	stmt.BindParams([]sql.NamedArg{
		sql.Named("intCol", 20),
		sql.Named("strCol", "hello world"),
	})

	r, err := g.Query(stmt)

	if err != nil {
		log.Println(err)
	}

	m := r.FetchMap()

	fmt.Println(m)
}

func doInsert(db *sql.DB) {
	g := gdo.New(db)

	stmt := gdo.Statement("INSERT INTO Test (IntCol, StringCol) VALUES (@intCol, @strCol)")

	stmt.BindParams([]sql.NamedArg{
		sql.Named("intCol", 11),
		sql.Named("strCol", randomString()),
	})

	r, err := g.Exec(stmt)

	if err != nil {
		log.Println(err)
	}

	id, err := r.LastInsertId()

	fmt.Println(id, err)
}

func doUpdate(db *sql.DB) {
	g := gdo.New(db)

	stmt := gdo.Statement("UPDATE Test SET `IntCol`=@intCol WHERE `id`=@id")
	stmt.BindParams([]sql.NamedArg{
		sql.Named("intCol", 1000),
		sql.Named("id", 1),
	})

	r, err := g.Exec(stmt)

	if err != nil {
		log.Println(err)
	}

	rows, err := r.RowsAffected()

	fmt.Println(rows, err)
}

func randomString() string {
	N := 100
	low := 65
	high := 122

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	b := make([]byte, N)

	for i := 0; i < N; i++ {
		b[i] = byte(r.Intn(high-low) + low)
	}

	return string(b)
}
