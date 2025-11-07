package demand

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	_ "github.com/lib/pq"
)

type PGStore struct {
	db *sql.DB
}

func NewPGStore(dsn string) (*PGStore, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &PGStore{db: db}, nil
}

func toJSON(v any) ([]byte, error) {
	if v == nil {
		return []byte("null"), nil
	}
	return json.Marshal(v)
}

// 1) 잡 생성
func (s *PGStore) CreateJob(ctx context.Context, job DBJob) error {
	cmdJSON, err := toJSON(job.Command)
	if err != nil {
		return err
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO demand_jobs (id, image, command, status, created_at)
		VALUES ($1, $2, $3, $4, COALESCE($5, now()))
		ON CONFLICT (id) DO NOTHING
	`, job.ID, job.Image, cmdJSON, job.Status, job.CreatedAt)
	return err
}

// 2) 잡 조회
func (s *PGStore) GetJob(ctx context.Context, id string) (*DBJob, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, image, command, status, created_at, retry_count
		  FROM demand_jobs
		 WHERE id = $1
	`, id)

	var (
		dbj     DBJob
		cmdJSON []byte
		status  string
	)
	if err := row.Scan(&dbj.ID, &dbj.Image, &cmdJSON, &status, &dbj.CreatedAt, &dbj.RetryCount); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrJobNotFound
		}
		return nil, err
	}
	dbj.Status = JobStatus(status)
	if len(cmdJSON) > 0 && string(cmdJSON) != "null" {
		_ = json.Unmarshal(cmdJSON, &dbj.Command)
	}
	return &dbj, nil
}

// 3) manifest 붙이기
func (s *PGStore) AttachManifest(ctx context.Context, id string, m Manifest) error {
	provJSON, err := toJSON(m.Providers)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE demand_jobs
		   SET manifest_root_cid  = $2,
		       manifest_providers = $3,
		       manifest_enc_meta  = $4
		 WHERE id = $1
	`, id, m.RootCID, provJSON, m.EncMeta)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrJobNotFound
	}
	return nil
}

// 4) try-claim
func (s *PGStore) TryClaim(ctx context.Context, id string, agentID string, ttl time.Duration) (*Lease, error) {
	row := s.db.QueryRowContext(ctx, `
		UPDATE demand_jobs
		   SET lease_agent      = $2,
		       lease_expires_at = now() + ($3 || ' seconds')::interval,
		       lease_token      = COALESCE(lease_token, 0) + 1,
		       status           = 'assigned'
		 WHERE id = $1
		   AND status = 'queued'  -- 이미 끝난 잡(succeeded)은 다시 못 잡게
		   AND (lease_expires_at IS NULL OR lease_expires_at < now())
		RETURNING lease_token, lease_expires_at
	`, id, agentID, int(ttl.Seconds()))

	var (
		token    int64
		expireAt time.Time
	)
	if err := row.Scan(&token, &expireAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// 왜 실패했는지 한 번 더 본다
			j, gerr := s.GetJob(ctx, id)
			if gerr != nil {
				return nil, gerr
			}
			if j == nil {
				return nil, ErrJobNotFound
			}

			// manifest 없거나 lease가 여전히 유효한 경우를 구분
			var rootCID sql.NullString
			var leaseValid bool
			r2 := s.db.QueryRowContext(ctx, `
				SELECT manifest_root_cid,
				       (lease_expires_at IS NOT NULL AND lease_expires_at >= now()) AS lease_valid
				  FROM demand_jobs
				 WHERE id = $1
			`, id)
			if err2 := r2.Scan(&rootCID, &leaseValid); err2 == nil {
				if !rootCID.Valid || rootCID.String == "" {
					return nil, ErrNoManifest
				}
				if leaseValid {
					return nil, ErrLeaseConflict
				}
			}
			return nil, ErrLeaseConflict
		}
		return nil, err
	}

	return &Lease{
		AgentID:      agentID,
		ExpireAt:     expireAt,
		FencingToken: uint64(token),
	}, nil
}

// 5) heartbeat
func (s *PGStore) Heartbeat(ctx context.Context, id string, agentID string, token uint64, ttl time.Duration) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE demand_jobs
		   SET lease_expires_at = now() + ($4 || ' seconds')::interval,
		       status           = 'running'
		 WHERE id = $1
		   AND lease_agent = $2
		   AND lease_token = $3
	`, id, agentID, int64(token), int(ttl.Seconds()))
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrBadToken
	}
	return nil
}

// 6) finish
func (s *PGStore) Finish(
	ctx context.Context,
	id string,
	agentID string,
	token uint64,
	succeeded bool,
	resultCID string,
	artifacts []string,
	metrics any,
) error {
	artsJSON, err := toJSON(artifacts)
	if err != nil {
		return err
	}
	metricsJSON, err := toJSON(metrics)
	if err != nil {
		return err
	}
	status := "failed"
	if succeeded {
		status = "succeeded"
	}

	_, err = s.db.ExecContext(ctx, `
		UPDATE demand_jobs
		   SET status           = $4,
		       result_root_cid  = $5,
		       artifacts        = $6,
		       metrics          = $7,
		       lease_agent      = NULL,
		       lease_expires_at = NULL
		 WHERE id = $1
		   AND (lease_agent = $2 OR lease_agent IS NULL)
		   AND lease_token <= $3
	`, id, agentID, int64(token), status, resultCID, artsJSON, metricsJSON)
	return err
}

// 7) 만료된 lease 목록
func (s *PGStore) ListExpiredLeases(ctx context.Context, now time.Time) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id
		  FROM demand_jobs
		 WHERE status IN ('assigned','running')
		   AND lease_expires_at IS NOT NULL
		   AND lease_expires_at < now()
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (s *PGStore) SetStatusQueued(ctx context.Context, id string) error {
    res, err := s.db.ExecContext(ctx, `
        UPDATE demand_jobs
           SET status = 'queued',
               lease_agent = NULL,
               lease_expires_at = NULL,
               retry_count = retry_count + 1
         WHERE id = $1
           AND status IN ('assigned', 'running')
           AND lease_expires_at IS NOT NULL
           AND lease_expires_at < now()
    `, id)
    if err != nil {
        return err
    }
    if n, _ := res.RowsAffected(); n == 0 {
        return ErrJobNotFound
    }
    return nil
}


func (s *PGStore) ListQueued(ctx context.Context) ([]DBJob, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, image, command, status, created_at
		  FROM demand_jobs
		 WHERE status = 'queued'
		 ORDER BY created_at ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DBJob
	for rows.Next() {
		var (
			j       DBJob
			cmdJSON []byte
			status  string
		)
		if err := rows.Scan(&j.ID, &j.Image, &cmdJSON, &status, &j.CreatedAt); err != nil {
			return nil, err
		}
		j.Status = JobStatus(status)
		if len(cmdJSON) > 0 && string(cmdJSON) != "null" {
			_ = json.Unmarshal(cmdJSON, &j.Command)
		}
		out = append(out, j)
	}
	return out, nil
}

func (s *PGStore) ListManifestMissingSince(ctx context.Context, cutoff time.Time) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id
		  FROM demand_jobs
		 WHERE status = 'failed'
		   AND (metrics->>'reason') = 'manifest_not_found'
		   AND created_at <= $1
	`, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (s *PGStore) ListAll(ctx context.Context) ([]DBJob, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, image, command, status, created_at, retry_count
		  FROM demand_jobs
		  ORDER BY created_at ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DBJob
	for rows.Next() {
		var (
			j       DBJob
			cmdJSON []byte
			status  string
		)
		if err := rows.Scan(&j.ID, &j.Image, &cmdJSON, &status, &j.CreatedAt, &j.RetryCount); err != nil {
			return nil, err
		}
		j.Status = JobStatus(status)
		if len(cmdJSON) > 0 && string(cmdJSON) != "null" {
			_ = json.Unmarshal(cmdJSON, &j.Command)
		}
		out = append(out, j)
	}
	return out, nil
}

// 페이지 단위 조회
func (s *PGStore) ListPaged(ctx context.Context, limit, offset int) ([]DBJob, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	if offset < 0 {
		offset = 0
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT id, image, command, status, created_at, retry_count
		  FROM demand_jobs
		 ORDER BY created_at DESC
		 LIMIT $1 OFFSET $2
	`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DBJob
	for rows.Next() {
		var (
			j       DBJob
			cmdJSON []byte
			status  string
		)
		if err := rows.Scan(&j.ID, &j.Image, &cmdJSON, &status, &j.CreatedAt, &j.RetryCount); err != nil {
			return nil, err
		}
		j.Status = JobStatus(status)
		if len(cmdJSON) > 0 && string(cmdJSON) != "null" {
			_ = json.Unmarshal(cmdJSON, &j.Command)
		}
		out = append(out, j)
	}
	return out, nil
}

// 상태 카운트
func (s *PGStore) CountByStatus(ctx context.Context) (map[JobStatus]int64, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT status, COUNT(*)
		  FROM demand_jobs
		 GROUP BY status
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[JobStatus]int64)
	for rows.Next() {
		var st string
		var cnt int64
		if err := rows.Scan(&st, &cnt); err != nil {
			return nil, err
		}
		out[JobStatus(st)] = cnt
	}
	return out, nil
}

