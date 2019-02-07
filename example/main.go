package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/Mehokm/gdo"

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

	doInsert(db)
	doInsertTx(db)
	doUpdate(db)

	doSelect(db)
	doSimpleSelect(db)
	doSelectRow(db)

	doPrepare(db)
	doPrepare2(db)
}

func doSimpleSelect(db *sql.DB) {
	g := gdo.New(db)

	stmt := gdo.NewStatement("SELECT * FROM Test")
	stmt.BindNamedArg(sql.Named("test", "foo"))

	r, err := g.Query(stmt)

	if err != nil {
		log.Println(err)
		return
	}

	r.FetchRows()

	fmt.Println(r.LastExecutedQuery())
}

func doSelect(db *sql.DB) {
	g := gdo.New(db)

	stmt := gdo.NewStatement("SELECT * FROM Test WHERE `IntCol` <> :col_int: AND `StringCol` = :col_str: AND `StringCol` <> :col:")

	stmt.BindNamedArgs([]sql.NamedArg{
		sql.Named("col_str", "good bye"),
		sql.Named("col", 10),
		sql.Named("col_int", 11),
	})

	r, err := g.Query(stmt)

	if err != nil {
		log.Println(err)
		return
	}

	m := r.FetchRows()

	fmt.Println(r.LastExecutedQuery())
	fmt.Println(m)
}

func doSelectRow(db *sql.DB) {
	g := gdo.New(db)

	stmt := gdo.NewStatement("SELECT * FROM Test WHERE `id`=:id:")

	stmt.BindNamedArgs([]sql.NamedArg{
		sql.Named("id", 2),
	})

	r := g.QueryRow(stmt)

	if r.LastError() != nil {
		log.Println(r.LastError())
		return
	}

	m := r.FetchRow()

	fmt.Println(r.LastExecutedQuery())
	fmt.Println(m)
}

func doInsert(db *sql.DB) {
	g := gdo.New(db)

	stmt := gdo.NewStatement("INSERT INTO Test (IntCol, StringCol) VALUES (:intCol:, :strCol:)")

	stmt.BindNamedArgs([]sql.NamedArg{
		sql.Named("intCol", 11),
		sql.Named("strCol", randomString()),
	})

	r, err := g.Exec(stmt)

	if err != nil {
		log.Println(err)
		return
	}

	id, err := r.LastInsertId()

	if err != nil {
		log.Println(err)
		return
	}

	fmt.Println(r.LastExecutedQuery())
	fmt.Println(id)
}

func doInsertTx(db *sql.DB) {
	g := gdo.New(db)

	tx, err := g.BeginTx()

	stmt := gdo.NewStatement("INSERT INTO Test (IntCol, StringCol) VALUES (:intCol:, :strCol:)")

	stmt.BindNamedArgs([]sql.NamedArg{
		sql.Named("intCol", 1101),
		sql.Named("strCol", randomString()),
	})

	r, err := tx.Exec(stmt)

	if err != nil {
		log.Println(err)
		return
	}

	id, err := r.LastInsertId()

	if err != nil {
		log.Println(err)
		return
	}

	err = tx.Commit()

	if err != nil {
		log.Println(err)
		return
	}

	fmt.Println(r.LastExecutedQuery())
	fmt.Println(id)
}

func doUpdate(db *sql.DB) {
	g := gdo.New(db)

	stmt := gdo.NewStatement("UPDATE Test SET `IntCol`=:intCol: WHERE `id`=:id:")
	stmt.BindNamedArgs([]sql.NamedArg{
		sql.Named("intCol", 10.10),
		sql.Named("id", 1),
	})

	r, err := g.Exec(stmt)

	if err != nil {
		log.Println(err)
		return
	}

	rows, err := r.RowsAffected()

	if err != nil {
		log.Println(err)
		return
	}

	fmt.Println(r.LastExecutedQuery())
	fmt.Println(rows)
}

func doPrepare(db *sql.DB) {
	g := gdo.New(db)

	p, _ := g.Prepare("SELECT * FROM Test WHERE `IntCol` <> :col_int: AND `StringCol` = :col_str: AND `StringCol` <> :col:")
	p.BindNamedArgs([]sql.NamedArg{
		sql.Named("col_str", "good bye"),
		sql.Named("col", 10),
	})
	p.BindNamedArg(sql.Named("col_int", 11))

	rs := p.QueryRow()

	fmt.Println(rs.LastExecutedQuery())
	fmt.Println(rs.FetchRow())

	p.Close()
}

func doPrepare2(db *sql.DB) {
	g := gdo.New(db)

	p, _ := g.Prepare("SELECT *, 'will this work?' AS test FROM Test WHERE `IntCol` <> ? AND `StringCol` = ? AND `StringCol` <> ?")

	p.BindArgs([]interface{}{
		11,
		"hello world",
	})
	p.BindArg(11)

	p.BindNamedArg(sql.Named("strCol", "hello world"))

	fmt.Println(p.QueryRow().FetchRow().String("StringCol"))
	fmt.Println(p.QueryRow().FetchRow().Int("IntCol"))
	fmt.Println(p.QueryRow().FetchRow().String("test"))

	p.Close()
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
