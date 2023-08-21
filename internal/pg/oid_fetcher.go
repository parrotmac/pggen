package pg

import (
	"context"
	"fmt"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
)

type OIDFetcher struct {
	cache   *typeCache
	querier *DBQuerier
}

func NewOIDFetcher(conn *pgx.Conn) *OIDFetcher {
	return &OIDFetcher{
		cache:   newTypeCache(),
		querier: NewQuerier(conn),
	}
}

const findTableNamesByOIDsQuery = `SELECT relname, oid FROM pg_class WHERE oid = ANY($1)`

func (p *OIDFetcher) FindTableNamesForOIDs(ctx context.Context, oids ...uint32) (map[pgtype.OID]string, error) {
	ctx = context.WithValue(ctx, "pggen_query_name", "FindTableNamesByOIDs")
	rows, err := p.querier.conn.Query(ctx, findTableNamesByOIDsQuery, oids)
	if err != nil {
		return nil, fmt.Errorf("find table names by oids: %w", err)
	}
	defer rows.Close()

	types := make(map[pgtype.OID]string)
	for rows.Next() {
		var name string
		var oid uint32
		if err := rows.Scan(&name, &oid); err != nil {
			return nil, fmt.Errorf("scan table name: %w", err)
		}
		types[pgtype.OID(oid)] = name
	}

	return types, nil
}
