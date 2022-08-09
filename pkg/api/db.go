package api

import (
	"bytes"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"time"
)

const defaultTTL = time.Hour * 24

type DB struct {
	db *bolt.DB
}

func NewDB() *DB {
	return &DB{}
}

func (d *DB) Init() error {
	db, err := bolt.Open("./store/kubemq-api.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
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
		_, err := tx.CreateBucketIfNotExists([]byte("snapshots"))
		if err != nil {
			return err
		}

		_, err = tx.CreateBucketIfNotExists([]byte("events_store"))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte("events"))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte("commands"))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte("queries"))
		if err != nil {
			return err
		}
		return nil
	})
}
func (d *DB) Close() error {
	return d.db.Close()
}
func (d *DB) SaveSnapshot(snapshot *Snapshot) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("snapshots"))
		if bucket == nil {
			return fmt.Errorf("bucket snapshots not found")
		}
		bytes, err := snapshot.ToBinary()
		if err != nil {
			return err
		}
		return bucket.Put([]byte(time.Unix(snapshot.Time, 0).Format(time.RFC3339)), bytes)
	})
}
func (d *DB) AddEntities(entities []*Entity) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		for _, entity := range entities {
			bucket := tx.Bucket([]byte(entity.Type))
			if bucket == nil {
				return fmt.Errorf("bucket %s not found", entity.Type)
			}
			bytes, err := entity.ToBinary()
			if err != nil {
				return err
			}
			err = bucket.Put([]byte(entity.Key()), bytes)
			if err != nil {
				return err
			}
			val := bucket.Get([]byte(entity.Key()))
			if val != nil {
				entity, err := EntityFromBinary(val)
				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Println(entity.String())
				}
			}
		}
		return nil
	})

}

func (d *DB) GetEntities(bucket, prefix string, fromTime int64) ([]*Entity, error) {
	var entities []*Entity
	err := d.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket([]byte(bucket)).Cursor()
		for k, v := cursor.Seek([]byte(prefix)); k != nil && bytes.HasPrefix(k, []byte(prefix)); k, v = cursor.Next() {
			entity, err := ParseEntity(v)
			if err != nil {
				return err
			}
			if entity.Time >= fromTime {
				entities = append(entities, entity)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entities, nil
}
