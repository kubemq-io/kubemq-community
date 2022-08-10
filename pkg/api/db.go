package api

import (
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
	db, err := bolt.Open(path+"/kubemq.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
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

func (d *DB) SaveEntitiesGroup(group *EntitiesGroup) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("entities"))
		data, err := group.ToBinary()
		if err != nil {
			return err
		}
		return b.Put([]byte(group.Key()), data)
	})
}
func (d *DB) SaveLastEntitiesGroup(group *EntitiesGroup) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("entities"))
		data, err := group.ToBinary()
		if err != nil {
			return err
		}
		return b.Put([]byte("__last__"), data)
	})
}

func (d *DB) GetLastEntities() (*EntitiesGroup, error) {
	var group *EntitiesGroup
	err := d.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("entities"))
		val := bucket.Get([]byte("__last__"))
		if val == nil {
			return fmt.Errorf("last entities not found")
		}
		var err error
		group, err = EntitiesGroupFromBinary(val)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return group, nil
}
