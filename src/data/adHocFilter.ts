import { getTable } from './ast';

export class AdHocFilter {
  private _targetTable = '';

  setTargetTableFromQuery(query: string) {
    this._targetTable = getTable(query);
    if (this._targetTable === '') {
      throw new Error('Failed to get table from adhoc query.');
    }
  }

  apply(sql: string, adHocFilters: AdHocVariableFilter[]): string {
    if (sql === '' || !adHocFilters || adHocFilters.length === 0) {
      return sql;
    }

    // sql can contain a query with double quotes around the database and table name, e.g. "default"."table", so we remove those
    if (this._targetTable !== '' && !sql.replace(/"/g, '').match(new RegExp(`.*\\b${this._targetTable}\\b.*`, 'gi'))) {
      return sql;
    }

    if (this._targetTable === '') {
      this._targetTable = getTable(sql);
    }

    if (this._targetTable === '') {
      return sql;
    }

    const filterClauses = adHocFilters
      .filter((filter: AdHocVariableFilter) => {
        const valid = isValid(filter);
        if (!valid) {
          console.warn('Invalid adhoc filter will be ignored:', filter);
        }
        return valid;
      })
      .map((f) => {
        const key = f.key.includes('.') ? `"${f.key.split('.')[1]}"` : `"${f.key}"`;
        const value = escapeValue(f.value, f.operator);
        const operator = convertOperator(f.operator);
        return `${key} ${operator} ${value}`;
      });

    if (filterClauses.length === 0) {
      return sql;
    }

    // Build the combined filter expression
    const filterExpr = filterClauses.join(' AND ');

    // Semicolons are not required and cause problems when building the SQL
    sql = sql.replace(';', '');

    // Inject WHERE clause into the SQL using standard SQL syntax.
    // Strategy: find the right insertion point in the query.
    return injectWhereClause(sql, filterExpr);
  }
}

/**
 * Injects a WHERE clause (or appends AND conditions) into a SQL query.
 * Handles queries with or without existing WHERE, GROUP BY, ORDER BY, LIMIT.
 */
function injectWhereClause(sql: string, filterExpr: string): string {
  // Check if there's already a WHERE clause (case-insensitive, word boundary).
  // We need to find the "main" WHERE, not one inside a subquery.
  // Simple heuristic: find WHERE that is NOT inside parentheses.
  const mainWhereIndex = findMainWhereIndex(sql);

  if (mainWhereIndex !== -1) {
    // There's already a WHERE clause — append our filters with AND.
    // Find the end of the WHERE clause (before GROUP BY, ORDER BY, LIMIT, or end of string).
    const insertPoint = findWhereEndIndex(sql, mainWhereIndex);
    const before = sql.substring(0, insertPoint).trimEnd();
    const after = sql.substring(insertPoint).trimStart();
    return after ? `${before} AND ${filterExpr} ${after}` : `${before} AND ${filterExpr}`;
  } else {
    // No WHERE clause — we need to insert one.
    // Find the insertion point: after FROM ... [JOIN ...] but before GROUP BY, ORDER BY, LIMIT.
    const insertPoint = findFromEndIndex(sql);
    const before = sql.substring(0, insertPoint).trimEnd();
    const after = sql.substring(insertPoint).trimStart();
    return after ? `${before} WHERE ${filterExpr} ${after}` : `${before} WHERE ${filterExpr}`;
  }
}

/**
 * Finds the index of the main WHERE keyword (not inside parentheses/subqueries).
 * Returns -1 if not found.
 */
function findMainWhereIndex(sql: string): number {
  let depth = 0;
  const upperSql = sql.toUpperCase();
  for (let i = 0; i < sql.length; i++) {
    if (sql[i] === '(') { depth++; }
    else if (sql[i] === ')') { depth--; }
    else if (depth === 0 && upperSql.substring(i, i + 6) === 'WHERE ' && (i === 0 || /\s/.test(sql[i - 1]))) {
      return i;
    }
  }
  return -1;
}

/**
 * Finds the end of the WHERE clause content (before GROUP BY, HAVING, ORDER BY, LIMIT, or end of string).
 * Skips over subqueries in parentheses.
 */
function findWhereEndIndex(sql: string, whereIndex: number): number {
  let depth = 0;
  const upperSql = sql.toUpperCase();
  // Start scanning after "WHERE "
  for (let i = whereIndex + 6; i < sql.length; i++) {
    if (sql[i] === '(') { depth++; }
    else if (sql[i] === ')') { depth--; }
    else if (depth === 0) {
      const remaining = upperSql.substring(i);
      if (/^(GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|$)/i.test(remaining)) {
        return i;
      }
    }
  }
  return sql.length;
}

/**
 * Finds the insertion point for a WHERE clause — after the FROM clause (and any JOINs)
 * but before GROUP BY, HAVING, ORDER BY, LIMIT, or end of string.
 * Skips over subqueries in parentheses.
 */
function findFromEndIndex(sql: string): number {
  let depth = 0;
  const upperSql = sql.toUpperCase();
  // Find FROM first
  let foundFrom = false;
  for (let i = 0; i < sql.length; i++) {
    if (sql[i] === '(') { depth++; }
    else if (sql[i] === ')') { depth--; }
    else if (depth === 0) {
      if (!foundFrom && upperSql.substring(i, i + 5) === 'FROM ' && (i === 0 || /\s/.test(sql[i - 1]))) {
        foundFrom = true;
        continue;
      }
      if (foundFrom) {
        const remaining = upperSql.substring(i);
        if (/^(GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|$)/i.test(remaining)) {
          return i;
        }
      }
    }
  }
  return sql.length;
}

function isValid(filter: AdHocVariableFilter): boolean {
  return filter.key !== undefined && filter.operator !== undefined && filter.value !== undefined;
}

function escapeValue(s: string, operator: AdHocVariableFilterOperator): string {
  if (operator === 'IN') {
    // Allow list of values without parentheses
    if (s.length > 2 && s[0] !== '(' && s[s.length - 1] !== ')') {
      s = `(${s})`;
    }
    return s;
  } else {
    return `'${s}'`;
  }
}

function convertOperator(operator: AdHocVariableFilterOperator): string {
  if (operator === '=~') {
    return 'ILIKE';
  }
  if (operator === '!~') {
    return 'NOT ILIKE';
  }
  return operator;
}

type AdHocVariableFilterOperator = '>' | '<' | '=' | '!=' | '=~' | '!~' | 'IN';

export type AdHocVariableFilter = {
  key: string;
  operator: AdHocVariableFilterOperator;
  value: string;
  condition?: string;
};
