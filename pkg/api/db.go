package api

import (
	"bytes"
	"encoding/gob"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"os"
	"time"
)

type DB struct {
	db *bolt.DB
}

func NewDB() *DB {
	return &DB{}
}

func (d *DB) Init(path string) error {
	// check if path exists , if not create it
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0755); err != nil {
			return err
		}
	}
	db, err := bolt.Open(path+"/kubemq.db", 0755, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	d.db = db
	if err := d.initBuckets(); err != nil {
		return err
	}

	return nil
}
func (d *DB) initBuckets() error {
	return d.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("entities"))
		if err != nil {
			return err
		}
		return nil
	})
}
func (d *DB) Close() error {
	return d.db.Close()
}

func (d *DB) SaveEntitiesGroups(ts int64, groups map[string]*EntitiesGroup) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(groups); err != nil {
		return err
	}

	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("entities"))
		return b.Put([]byte(time.UnixMilli(ts).Format(time.RFC3339)), buf.Bytes())
	})
}
func (d *DB) SaveLastEntitiesGroup(groups map[string]*EntitiesGroup) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(groups); err != nil {
		return err
	}
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("entities"))
		return b.Put([]byte("__last__"), buf.Bytes())
	})
}

func (d *DB) GetLastEntities() (map[string]*EntitiesGroup, error) {
	var groups map[string]*EntitiesGroup
	err := d.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("entities"))
		val := bucket.Get([]byte("__last__"))
		if val == nil {
			return fmt.Errorf("last entities not found")
		}

		buf := bytes.NewBuffer(val)
		dec := gob.NewDecoder(buf)
		if err := dec.Decode(&groups); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return groups, nil
}
