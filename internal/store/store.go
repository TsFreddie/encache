package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"

	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

type MediaSource struct {
	MediaSourceID string
	ItemID        string
	ItemName      string
	SourceName    string
	Size          int64
	Container     string
	Bitrate       int64
	Chunks        []byte
	CreatedAt     string
	UpdatedAt     string
}

func Open(ctx context.Context, storagePath string) (*Store, error) {
	dbPath := filepath.Join(storagePath, "metadata.sqlite")
	dsn := fmt.Sprintf(
		"file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)",
		dbPath,
	)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}

	store := &Store{db: db}
	if err := store.init(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) init(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS media_sources (
  media_source_id TEXT NOT NULL PRIMARY KEY,
  item_id TEXT NOT NULL,
  item_name TEXT NOT NULL,
  source_name TEXT NOT NULL,
  size INTEGER NOT NULL,
  container TEXT NOT NULL,
  bitrate INTEGER NOT NULL,
  chunks BLOB,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS media_sources_item_id_idx ON media_sources (item_id);
`)
	return err
}

func (s *Store) InsertMediaSource(ctx context.Context, source MediaSource) (bool, error) {
	if !validMediaSource(source) {
		return false, nil
	}

	result, err := s.db.ExecContext(ctx, `
INSERT OR IGNORE INTO media_sources (
  media_source_id,
  item_id,
  item_name,
  source_name,
  size,
  container,
  bitrate
) VALUES (?, ?, ?, ?, ?, ?, ?)
`, source.MediaSourceID, source.ItemID, source.ItemName, source.SourceName, source.Size, source.Container, source.Bitrate)
	if err != nil {
		return false, err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func (s *Store) UpsertMediaSource(ctx context.Context, source MediaSource) (affected bool, updated bool, oldItemName string, oldSourceName string, oldContainer string, err error) {
	if !validMediaSource(source) {
		return false, false, "", "", "", nil
	}

	var itemName, sourceName, container string
	var size, bitrate int64
	err = s.db.QueryRowContext(ctx, `
SELECT item_name, source_name, size, container, bitrate
FROM media_sources
WHERE media_source_id = ?
`, source.MediaSourceID).Scan(&itemName, &sourceName, &size, &container, &bitrate)

	if err == nil {
		if itemName == source.ItemName &&
			sourceName == source.SourceName &&
			size == source.Size &&
			container == source.Container &&
			bitrate == source.Bitrate {
			return false, false, "", "", "", nil
		}
		result, err := s.db.ExecContext(ctx, `
UPDATE media_sources
SET item_name = ?, source_name = ?, size = ?, container = ?, bitrate = ?, updated_at = CURRENT_TIMESTAMP
WHERE media_source_id = ?
`, source.ItemName, source.SourceName, source.Size, source.Container, source.Bitrate, source.MediaSourceID)
		if err != nil {
			return false, false, "", "", "", err
		}
		rows, _ := result.RowsAffected()
		return rows > 0, true, itemName, sourceName, container, nil
	}

	if err != sql.ErrNoRows {
		return false, false, "", "", "", err
	}

	result, err := s.db.ExecContext(ctx, `
INSERT INTO media_sources (
  media_source_id,
  item_id,
  item_name,
  source_name,
  size,
  container,
  bitrate
) VALUES (?, ?, ?, ?, ?, ?, ?)
`, source.MediaSourceID, source.ItemID, source.ItemName, source.SourceName, source.Size, source.Container, source.Bitrate)
	if err != nil {
		return false, false, "", "", "", err
	}
	rows, _ := result.RowsAffected()
	return rows > 0, false, "", "", "", nil
}

func (s *Store) GetMediaSource(ctx context.Context, mediaSourceID string) (MediaSource, bool, error) {
	var source MediaSource
	err := s.db.QueryRowContext(ctx, `
SELECT media_source_id, item_id, item_name, source_name, size, container, bitrate, chunks, created_at, updated_at
FROM media_sources
WHERE media_source_id = ?
`, mediaSourceID).Scan(
		&source.MediaSourceID,
		&source.ItemID,
		&source.ItemName,
		&source.SourceName,
		&source.Size,
		&source.Container,
		&source.Bitrate,
		&source.Chunks,
		&source.CreatedAt,
		&source.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return MediaSource{}, false, nil
	}
	if err != nil {
		return MediaSource{}, false, err
	}
	return source, true, nil
}

func (s *Store) GetPreferredMediaSourceByItemID(ctx context.Context, itemID string) (MediaSource, bool, error) {
	var source MediaSource
	err := s.db.QueryRowContext(ctx, `
SELECT media_source_id, item_id, item_name, source_name, size, container, bitrate, chunks, created_at, updated_at
FROM media_sources
WHERE item_id = ?
ORDER BY bitrate DESC, size DESC
LIMIT 1
`, itemID).Scan(
		&source.MediaSourceID,
		&source.ItemID,
		&source.ItemName,
		&source.SourceName,
		&source.Size,
		&source.Container,
		&source.Bitrate,
		&source.Chunks,
		&source.CreatedAt,
		&source.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return MediaSource{}, false, nil
	}
	if err != nil {
		return MediaSource{}, false, err
	}
	return source, true, nil
}

func (s *Store) UpdateChunks(ctx context.Context, mediaSourceID string, chunks []byte) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE media_sources
SET chunks = ?, updated_at = CURRENT_TIMESTAMP
WHERE media_source_id = ?
`, chunks, mediaSourceID)
	return err
}

func validMediaSource(source MediaSource) bool {
	if source.MediaSourceID == "" || source.ItemID == "" || source.ItemName == "" || source.SourceName == "" {
		return false
	}
	if source.Size <= 0 || source.Bitrate < 0 || source.Container == "" {
		return false
	}
	return source.Container != "m3u8" && source.Container != "hls"
}
