//! Safe SQL query builder for parameterized queries
//!
//! This module provides a type-safe query builder that prevents SQL injection
//! by using parameterized queries instead of string formatting.

use sqlx::postgres::PgArguments;
use sqlx::Arguments;

/// A builder for constructing safe parameterized SQL queries
/// 
/// # Example
/// 
/// ```rust
/// let mut builder = QueryBuilder::new("SELECT * FROM messages WHERE 1=1");
/// 
/// if let Some(sender) = &params.sender {
///     builder.and_eq("sender", sender);
/// }
/// 
/// builder.order_by("time", false);
/// builder.limit(100);
/// 
/// let (query, args) = builder.build();
/// sqlx::query_with(&query, args).fetch_all(pool).await?;
/// ```
pub struct QueryBuilder {
    query: String,
    count_query: String,
    args: PgArguments,
    param_index: i32,
}

impl std::fmt::Debug for QueryBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryBuilder")
            .field("query", &self.query)
            .field("count_query", &self.count_query)
            .field("param_index", &self.param_index)
            .finish()
    }
}

impl QueryBuilder {
    /// Create a new query builder with a base query
    /// 
    /// The base query should include any initial WHERE conditions.
    /// Typically use "SELECT ... WHERE 1=1" to allow easy AND appending.
    pub fn new(base_query: &str) -> Self {
        Self {
            query: base_query.to_string(),
            count_query: Self::to_count_query(base_query),
            args: PgArguments::default(),
            param_index: 1,
        }
    }
    
    /// Convert a SELECT query to a COUNT query
    fn to_count_query(query: &str) -> String {
        // Find the FROM clause and replace everything before it with SELECT COUNT(*)
        if let Some(from_pos) = query.to_uppercase().find(" FROM ") {
            format!("SELECT COUNT(*){}", &query[from_pos..])
        } else {
            query.to_string()
        }
    }
    
    /// Add an AND equality condition
    pub fn and_eq<T: sqlx::Encode<'static, sqlx::Postgres> + sqlx::Type<sqlx::Postgres> + Send + 'static>(
        &mut self,
        column: &str,
        value: T,
    ) -> &mut Self {
        let clause = format!(" AND {} = ${}", column, self.param_index);
        self.query.push_str(&clause);
        self.count_query.push_str(&clause);
        let _ = self.args.add(value);
        self.param_index += 1;
        self
    }
    
    /// Add an AND IN condition for multiple values
    pub fn and_in<T>(
        &mut self,
        column: &str,
        values: &[T],
    ) -> &mut Self 
    where
        T: sqlx::Encode<'static, sqlx::Postgres> + sqlx::Type<sqlx::Postgres> + Send + Clone + 'static,
    {
        if values.is_empty() {
            return self;
        }
        
        let placeholders: Vec<String> = (0..values.len())
            .map(|i| format!("${}", self.param_index + i as i32))
            .collect();
        
        let clause = format!(" AND {} IN ({})", column, placeholders.join(", "));
        self.query.push_str(&clause);
        self.count_query.push_str(&clause);
        
        for value in values {
            let _ = self.args.add(value.clone());
            self.param_index += 1;
        }
        
        self
    }
    
    /// Add an AND LIKE condition (case-insensitive with ILIKE)
    pub fn and_ilike(&mut self, column: &str, pattern: &str) -> &mut Self {
        let clause = format!(" AND {} ILIKE ${}", column, self.param_index);
        self.query.push_str(&clause);
        self.count_query.push_str(&clause);
        let _ = self.args.add(pattern.to_string());
        self.param_index += 1;
        self
    }
    
    /// Add an AND >= condition
    pub fn and_gte<T: sqlx::Encode<'static, sqlx::Postgres> + sqlx::Type<sqlx::Postgres> + Send + 'static>(
        &mut self,
        column: &str,
        value: T,
    ) -> &mut Self {
        let clause = format!(" AND {} >= ${}", column, self.param_index);
        self.query.push_str(&clause);
        self.count_query.push_str(&clause);
        let _ = self.args.add(value);
        self.param_index += 1;
        self
    }
    
    /// Add an AND <= condition
    pub fn and_lte<T: sqlx::Encode<'static, sqlx::Postgres> + sqlx::Type<sqlx::Postgres> + Send + 'static>(
        &mut self,
        column: &str,
        value: T,
    ) -> &mut Self {
        let clause = format!(" AND {} <= ${}", column, self.param_index);
        self.query.push_str(&clause);
        self.count_query.push_str(&clause);
        let _ = self.args.add(value);
        self.param_index += 1;
        self
    }

    
    /// Add a raw AND condition (use carefully - values in the clause are NOT parameterized)
    ///
    /// This is intended for subqueries or complex conditions that cannot be expressed
    /// with the typed methods. The caller is responsible for ensuring the clause is safe.
    pub fn and_raw(&mut self, clause: &str) -> &mut Self {
        self.query.push_str(" AND ");
        self.query.push_str(clause);
        self.count_query.push_str(" AND ");
        self.count_query.push_str(clause);
        self
    }

    /// Add an AND condition for JSONB text field equality
    /// Casts TEXT to JSONB and extracts the field value
    pub fn and_jsonb_text_eq(&mut self, column: &str, json_path: &str, value: String) -> &mut Self {
        // Validate column and path names
        if !column.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.') {
            panic!("Invalid column name: {}", column);
        }
        if !json_path.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.' || c == '.') {
            panic!("Invalid json path: {}", json_path);
        }
        
        let clause = format!(" AND ({}::jsonb->>'{}') = ${}", column, json_path, self.param_index);
        self.query.push_str(&clause);
        self.count_query.push_str(&clause);
        let _ = self.args.add(value);
        self.param_index += 1;
        self
    }
    
    /// Add an AND IN condition for JSONB text field
    /// Add an AND IN condition for JSONB text field
    /// Supports nested paths like "content.type"
    pub fn and_jsonb_text_in(&mut self, column: &str, json_path: &str, values: &[String]) -> &mut Self {
        if values.is_empty() {
            return self;
        }
        
        // Validate column and path names
        if !column.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.') {
            panic!("Invalid column name: {}", column);
        }
        if !json_path.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.' || c == '.') {
            panic!("Invalid json path: {}", json_path);
        }
        
        let placeholders: Vec<String> = (0..values.len())
            .map(|i| format!("${}", self.param_index + i as i32))
            .collect();
        
        // Handle nested paths - convert "content.type" to path array
        let path_parts: Vec<&str> = json_path.split('.').collect();
        let clause = if path_parts.len() > 1 {
            // Nested path - use #>> operator with path array format
            let path_array = path_parts.join(",");
            format!(" AND ({}::jsonb #>> '{{{}}}') IN ({})", column, path_array, placeholders.join(", "))
        } else {
            // Simple path
            format!(" AND ({}::jsonb->>'{}') IN ({})", column, json_path, placeholders.join(", "))
        };
        self.query.push_str(&clause);
        self.count_query.push_str(&clause);
        
        for value in values {
            let _ = self.args.add(value.clone());
            self.param_index += 1;
        }
        
        self
    }
    
    /// Add an AND condition checking if JSONB array contains a value
    /// Add an AND condition checking if JSONB array contains a value
    /// Supports nested paths like "content.tags"
    pub fn and_jsonb_array_contains(&mut self, column: &str, json_path: &str, value: String) -> &mut Self {
        // Validate column and path names
        if !column.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.') {
            panic!("Invalid column name: {}", column);
        }
        if !json_path.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.' || c == '.') {
            panic!("Invalid json path: {}", json_path);
        }
        
        // Handle nested paths - convert "content.tags" to path array
        let path_parts: Vec<&str> = json_path.split('.').collect();
        let clause = if path_parts.len() > 1 {
            // Nested path - use #> operator with path array
            let path_array = path_parts.join(",");
            format!(" AND ({}::jsonb #> '{{{}}}') ? ${}", column, path_array, self.param_index)
        } else {
            // Simple path - use -> operator
            format!(" AND ({}::jsonb->'{}') ? ${}", column, json_path, self.param_index)
        };
        self.query.push_str(&clause);
        self.count_query.push_str(&clause);
        let _ = self.args.add(value);
        self.param_index += 1;
        self
    }
    
    /// Add ORDER BY clause
    ///
    /// # Arguments
    ///
    /// * `column` - Column to order by (must be a valid column name)
    /// * `ascending` - true for ASC, false for DESC
    pub fn order_by(&mut self, column: &str, ascending: bool) -> &mut Self {
        // Validate column name to prevent injection (alphanumeric and underscore only)
        if !column.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.') {
            panic!("Invalid column name: {}", column);
        }

        let direction = if ascending { "ASC" } else { "DESC" };
        self.query.push_str(&format!(" ORDER BY {} {}", column, direction));
        self
    }

    /// Add a raw ORDER BY clause for complex multi-column sorting
    ///
    /// The clause should NOT include "ORDER BY" — just the column expressions.
    /// Only safe for static/hardcoded clauses, not user input.
    pub fn order_by_raw(&mut self, clause: &str) -> &mut Self {
        self.query.push_str(&format!(" ORDER BY {}", clause));
        self
    }
    
    /// Add LIMIT clause
    pub fn limit(&mut self, limit: i64) -> &mut Self {
        self.query.push_str(&format!(" LIMIT ${}", self.param_index));
        let _ = self.args.add(limit);
        self.param_index += 1;
        self
    }
    
    /// Add OFFSET clause
    pub fn offset(&mut self, offset: i64) -> &mut Self {
        self.query.push_str(&format!(" OFFSET ${}", self.param_index));
        let _ = self.args.add(offset);
        self.param_index += 1;
        self
    }
    
    /// Get the main query string
    pub fn query(&self) -> &str {
        &self.query
    }
    
    /// Get the count query string
    pub fn count_query(&self) -> &str {
        &self.count_query
    }
    
    /// Build and consume the builder, returning the query and arguments
    pub fn build(self) -> (String, PgArguments) {
        (self.query, self.args)
    }
    
    /// Build and return separate queries for data and count
    pub fn build_with_count(self) -> (String, String, PgArguments) {
        (self.query, self.count_query, self.args)
    }
}

/// Helper to parse comma-separated values from query params
pub fn parse_csv_param(param: &str) -> Vec<String> {
    param
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Helper to validate and sanitize a sort column name
pub fn validate_sort_column<'a>(column: &str, allowed: &[&'a str]) -> Option<&'a str> {
    allowed.iter().find(|&&c| c == column).copied()
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_basic_query() {
        let builder = QueryBuilder::new("SELECT * FROM messages WHERE 1=1");
        assert!(builder.query().contains("SELECT * FROM messages"));
    }
    
    #[test]
    fn test_parse_csv_param() {
        let values = parse_csv_param("a, b, c");
        assert_eq!(values, vec!["a", "b", "c"]);
        
        let values = parse_csv_param("");
        assert!(values.is_empty());
        
        let values = parse_csv_param("single");
        assert_eq!(values, vec!["single"]);
    }
    
    #[test]
    fn test_validate_sort_column() {
        let allowed = &["time", "sender", "channel"];
        
        assert_eq!(validate_sort_column("time", allowed), Some("time"));
        assert_eq!(validate_sort_column("invalid", allowed), None);
    }
    
    #[test]
    #[should_panic(expected = "Invalid column name")]
    fn test_order_by_injection_prevention() {
        let mut builder = QueryBuilder::new("SELECT * FROM messages WHERE 1=1");
        builder.order_by("time; DROP TABLE messages; --", true);
    }
}
