package db

import (
	"context"
	"database/sql"
	"fmt"
)

// BranchRunSnapshot holds one run and its pinned plan inputs from the same read
// transaction as the complete branch inventory. It contains no live Git facts.
type BranchRunSnapshot struct {
	Run       *Run
	GatesJSON string
	Steps     []*StepResult
}

// GetBranchSnapshot reads every run on exactly one repository and branch,
// newest first under (created_at DESC, id DESC), and their step states in one
// SQLite snapshot. No active-run preference or inventory limit is applied.
func (d *DB) GetBranchSnapshot(ctx context.Context, repoID, branch string) ([]BranchRunSnapshot, error) {
	tx, err := d.sql.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin branch snapshot: %w", err)
	}
	defer tx.Rollback()
	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT 1 FROM repos WHERE id = ?`, repoID).Scan(&exists); err != nil {
		return nil, fmt.Errorf("branch snapshot repository: %w", err)
	}
	// Select only snapshot facts. Optional launch metadata added in newer
	// releases must not require a migration just to read publication history.
	rows, err := tx.QueryContext(ctx, `SELECT id, repo_id, branch, head_sha, review_approved_head_sha, status, pr_url, last_pushed_sha, push_target_kind, push_target_fingerprint, push_ref, last_pushed_at, push_generation, COALESCE(push_active, 0), created_at FROM runs WHERE repo_id = ? AND branch = ? ORDER BY created_at DESC, id DESC`, repoID, branch)
	if err != nil {
		return nil, fmt.Errorf("read branch runs: %w", err)
	}
	out := make([]BranchRunSnapshot, 0)
	for rows.Next() {
		run := &Run{}
		if err := rows.Scan(&run.ID, &run.RepoID, &run.Branch, &run.HeadSHA, &run.ReviewApprovedHeadSHA, &run.Status, &run.PRURL, &run.LastPushedSHA, &run.PushTargetKind, &run.PushTargetFingerprint, &run.PushRef, &run.LastPushedAt, &run.PushGeneration, &run.PushActive, &run.CreatedAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("read branch run: %w", err)
		}
		out = append(out, BranchRunSnapshot{Run: run})
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, fmt.Errorf("read branch runs: %w", err)
	}
	// As in GetStepsByRun, legacy rows can predate optional outcome reasons.
	// Inspect schema within this same transaction; unavailable reasons stay NULL.
	// Only the newest run needs Test evidence for its terminal outcome. Older
	// inventory rows expose phase and status, not findings or outcomes.
	newestID := ""
	if len(out) > 0 {
		newestID = out[0].Run.ID
	}
	stepColumns := "id, run_id, step_name, step_order, status, CASE WHEN step_name = 'test' AND run_id = ? THEN findings_json ELSE NULL END"
	// Validate the required table even for an empty branch. Missing step
	// storage must not become a success-shaped empty snapshot.
	requiredSteps, err := tx.QueryContext(ctx, `SELECT `+stepColumns+` FROM step_results WHERE 0`, newestID)
	if err != nil {
		return nil, fmt.Errorf("read step schema: %w", err)
	}
	requiredSteps.Close()
	for _, column := range []string{"override_reason", "approval_reason", "skip_reason"} {
		var present int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('step_results') WHERE name = ?`, column).Scan(&present); err != nil {
			return nil, fmt.Errorf("read step schema: %w", err)
		}
		if present == 0 {
			stepColumns += ", NULL"
		} else {
			stepColumns += ", " + column
		}
	}
	// A database from before repository gates has no pin column and can
	// describe only the core plan, just like an absent pin on a legacy run.
	gatesColumn := "gates_json"
	var hasGates int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('runs') WHERE name = 'gates_json'`).Scan(&hasGates); err != nil {
		return nil, fmt.Errorf("read gate schema: %w", err)
	}
	if hasGates == 0 {
		gatesColumn = "NULL"
	}
	for i := range out {
		snapshot := &out[i]
		var gates sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT `+gatesColumn+` FROM runs WHERE id = ?`, snapshot.Run.ID).Scan(&gates); err != nil {
			return nil, fmt.Errorf("read run gates: %w", err)
		}
		snapshot.GatesJSON = gates.String
		// These are the durable fields needed for phase, plan and outcome
		// reporting. Test evidence is included only to preserve existing
		// outcome qualification; logs and round statistics are not sampled.
		rows, err := tx.QueryContext(ctx, `SELECT `+stepColumns+` FROM step_results WHERE run_id = ? ORDER BY step_order, id`, newestID, snapshot.Run.ID)
		if err != nil {
			return nil, fmt.Errorf("read branch steps: %w", err)
		}
		for rows.Next() {
			step := &StepResult{}
			if err := rows.Scan(&step.ID, &step.RunID, &step.StepName, &step.StepOrder, &step.Status, &step.FindingsJSON, &step.OverrideReason, &step.ApprovalReason, &step.SkipReason); err != nil {
				rows.Close()
				return nil, fmt.Errorf("read branch step: %w", err)
			}
			snapshot.Steps = append(snapshot.Steps, step)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, fmt.Errorf("read branch steps: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("finish branch snapshot: %w", err)
	}
	return out, nil
}
