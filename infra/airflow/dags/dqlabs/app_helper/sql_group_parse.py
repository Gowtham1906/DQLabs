import re
from typing import List, Optional, Tuple


class SQLGroupByProcessor:
    """
    A generic SQL query processor that can apply GROUP BY clauses to complex queries
    including CTEs, subqueries, joins, and multiple SELECT statements.
    """
    
    def __init__(self):
        pass
        
    def apply_group_by(self, query: str, group_by_column: str, 
                      aggregate_functions: Optional[List[str]] = None) -> str:
        """
        Apply GROUP BY clause to a SQL query.
        
        Args:
            query: The SQL query string
            group_by_column: The column name to group by
            aggregate_functions: Optional list of aggregate functions to apply
                                (e.g., ['COUNT(*)', 'SUM(amount)', 'AVG(price)'])
        
        Returns:
            Modified query with GROUP BY clause applied
            
        Raises:
            ValueError: If the query is invalid or cannot be processed
        """
        # Input validation
        if not query or not query.strip():
            raise ValueError("Query cannot be empty")
        
        if not group_by_column or not group_by_column.strip():
            raise ValueError("Group by column cannot be empty")
        
        if aggregate_functions is not None:
            if not isinstance(aggregate_functions, list):
                raise ValueError("Aggregate functions must be a list")
            for func in aggregate_functions:
                if not isinstance(func, str) or not func.strip():
                    raise ValueError("All aggregate functions must be non-empty strings")
        
        try:
            # Clean and normalize the query
            query = self._clean_query(query)
            
            # Parse the query structure
            query_parts = self._parse_query_structure(query)
            
            # Apply GROUP BY to the main SELECT
            modified_query = self._apply_group_by_to_query(query_parts, group_by_column, aggregate_functions)
            
            return modified_query
            
        except Exception as e:
            raise ValueError(f"Error processing query: {str(e)}")
    
    def _clean_query(self, query: str) -> str:
        """Clean and normalize the SQL query."""
        # Remove extra whitespace and normalize
        query = re.sub(r'\s+', ' ', query.strip())
        
        # Ensure query ends with semicolon or add one
        if not query.endswith(';'):
            query += ';'
            
        return query
    
    def _parse_query_structure(self, query: str) -> dict:
        """Parse the query into its structural components using a more robust approach."""
        
        # Find the main SELECT statement by looking for the last SELECT that's not in a CTE
        query_upper = query.upper()
        
        # Split by WITH and find CTEs
        ctes = []
        main_query = query
        
        if 'WITH ' in query_upper:
            # Use a more robust approach to find CTEs
            # Find the position of the main SELECT (after all CTEs)
            with_pos = query_upper.find('WITH ')
            if with_pos != -1:
                # Find the end of the CTE section by looking for the main SELECT
                # that comes after the CTE closing parenthesis
                cte_section = query[with_pos:]
                
                # Find the main SELECT that's not inside a CTE
                main_select_pos = self._find_main_select_position(cte_section)
                
                if main_select_pos != -1:
                    # Extract CTE part
                    cte_part = cte_section[:main_select_pos].strip()
                    ctes.append(cte_part)
                    
                    # Extract main query part
                    main_query = cte_section[main_select_pos:].strip()
        
        # Now parse the main query structure
        # Find SELECT clause using balanced parentheses approach
        main_select = self._extract_select_clause(main_query)
        if not main_select:
            raise ValueError("No SELECT statement found in query")
        
        # Find other clauses in the main query
        from_clause = self._extract_clause_from_main(main_query, 'FROM')
        where_clause = self._extract_clause_from_main(main_query, 'WHERE')
        group_by_clause = self._extract_clause_from_main(main_query, 'GROUP BY')
        having_clause = self._extract_clause_from_main(main_query, 'HAVING')
        order_by_clause = self._extract_clause_from_main(main_query, 'ORDER BY')
        limit_clause = self._extract_clause_from_main(main_query, 'LIMIT')
        offset_clause = self._extract_clause_from_main(main_query, 'OFFSET')
        fetch_clause = self._extract_clause_from_main(main_query, 'FETCH')
        
        return {
            'ctes': ctes,
            'main_select': main_select,
            'from_clause': from_clause,
            'where_clause': where_clause,
            'group_by_clause': group_by_clause,
            'having_clause': having_clause,
            'order_by_clause': order_by_clause,
            'limit_clause': limit_clause,
            'offset_clause': offset_clause,
            'fetch_clause': fetch_clause,
            'original_query': query
        }
    
    def _extract_select_clause(self, query: str) -> Optional[str]:
        """
        Extract the SELECT clause from a query, handling complex expressions
        like window functions with WITHIN GROUP and OVER clauses.
        """
        query_upper = query.upper()
        
        # Find SELECT keyword
        select_match = re.search(r'\bSELECT\b', query_upper)
        if not select_match:
            return None
        
        select_start = select_match.start()
        
        # Find the FROM keyword that's at the same nesting level
        # by tracking parentheses
        pos = select_start + len('SELECT')
        paren_depth = 0
        from_pos = -1
        
        while pos < len(query):
            char = query[pos]
            
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            elif paren_depth == 0:
                # We're at the top level, check for FROM
                remaining = query_upper[pos:]
                if remaining.startswith('FROM'):
                    from_pos = pos
                    break
            
            pos += 1
        
        if from_pos != -1:
            return query[select_start:from_pos].strip()
        else:
            # No FROM found, return everything from SELECT onwards
            return query[select_start:].strip()
    
    def _find_main_select_position(self, query_section: str) -> int:
        """Find the position of the main SELECT statement (not inside a CTE)."""
        query_upper = query_section.upper()
        
        # Look for SELECT statements
        select_positions = []
        for match in re.finditer(r'\bSELECT\b', query_upper):
            select_positions.append(match.start())
        
        # Find the SELECT that's not inside a CTE
        for pos in select_positions:
            # Check if this SELECT is inside a CTE by counting parentheses
            before_select = query_section[:pos]
            
            # Count opening and closing parentheses
            open_count = before_select.count('(')
            close_count = before_select.count(')')
            
            # If parentheses are balanced, this is likely the main SELECT
            if open_count == close_count:
                return pos
        
        return -1
    
    def _extract_clause_from_main(self, query: str, clause_name: str) -> Optional[str]:
        """
        Extract a specific SQL clause from the main query.
        Uses a more robust approach that handles nested parentheses and subqueries.
        """
        query_upper = query.upper()
        
        # Find the clause start position
        if clause_name == 'FROM':
            start_pattern = r'\bFROM\b'
            end_keywords = ['WHERE', 'GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT', 'OFFSET', 'FETCH']
        elif clause_name == 'WHERE':
            start_pattern = r'\bWHERE\b'
            end_keywords = ['GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT', 'OFFSET', 'FETCH']
        elif clause_name == 'GROUP BY':
            start_pattern = r'\bGROUP\s+BY\b'
            end_keywords = ['HAVING', 'ORDER BY', 'LIMIT', 'OFFSET', 'FETCH']
        elif clause_name == 'HAVING':
            start_pattern = r'\bHAVING\b'
            end_keywords = ['ORDER BY', 'LIMIT', 'OFFSET', 'FETCH']
        elif clause_name == 'ORDER BY':
            start_pattern = r'\bORDER\s+BY\b'
            end_keywords = ['LIMIT', 'OFFSET', 'FETCH']
        elif clause_name == 'LIMIT':
            start_pattern = r'\bLIMIT\b'
            end_keywords = ['OFFSET', 'FETCH']
        elif clause_name == 'OFFSET':
            start_pattern = r'\bOFFSET\b'
            end_keywords = ['FETCH']
        elif clause_name == 'FETCH':
            start_pattern = r'\bFETCH\b'
            end_keywords = []
        else:
            return None
        
        # Find the start of the clause
        start_match = re.search(start_pattern, query_upper)
        if not start_match:
            return None
        
        start_pos = start_match.start()
        
        # Find the end of the clause by looking for the next keyword at the same nesting level
        end_pos = len(query)
        
        for keyword in end_keywords:
            keyword_pattern = r'\b' + keyword.replace(' ', r'\s+') + r'\b'
            # Find all occurrences of this keyword
            for match in re.finditer(keyword_pattern, query_upper):
                candidate_pos = match.start()
                if candidate_pos > start_pos:
                    # Check if this keyword is at the same nesting level
                    # by counting parentheses between start and this position
                    substring = query[start_pos:candidate_pos]
                    if self._is_balanced_parentheses(substring):
                        # This keyword is at the same level
                        end_pos = min(end_pos, candidate_pos)
                        break
        
        # Also check for semicolon
        semicolon_pos = query.find(';', start_pos)
        if semicolon_pos != -1:
            # Make sure the semicolon is at the same nesting level
            substring = query[start_pos:semicolon_pos]
            if self._is_balanced_parentheses(substring):
                end_pos = min(end_pos, semicolon_pos)
        
        return query[start_pos:end_pos].strip()
    
    def _is_balanced_parentheses(self, text: str) -> bool:
        """
        Check if parentheses are balanced in the given text.
        Returns True if balanced (same number of open and close parentheses).
        """
        count = 0
        for char in text:
            if char == '(':
                count += 1
            elif char == ')':
                count -= 1
        return count == 0
    
    def _apply_group_by_to_query(self, query_parts: dict, group_by_column: str, 
                                aggregate_functions: Optional[List[str]]) -> str:
        """Apply GROUP BY clause to the parsed query structure."""
        
        # Check if GROUP BY already exists
        if query_parts['group_by_clause']:
            # If GROUP BY exists, add the new column to it
            existing_group_by = query_parts['group_by_clause']
            new_group_by = f"{existing_group_by}, {group_by_column}"
        else:
            # Create new GROUP BY clause
            new_group_by = f"GROUP BY {group_by_column}"
        
        # Modify SELECT clause if aggregate functions are provided
        modified_select = query_parts['main_select']
        if aggregate_functions:
            # Extract the SELECT part without SELECT keyword
            select_content = re.sub(r'^\s*SELECT\s+', '', modified_select, flags=re.IGNORECASE)
            
            # If SELECT * is used, we need to replace it with specific columns
            if select_content.strip() == '*':
                # For SELECT *, replace * with group by column and aggregate functions
                new_select_content = f"{group_by_column}, {', '.join(aggregate_functions)}"
            else:
                # Add aggregate functions to existing SELECT
                new_select_content = f"{group_by_column}, {', '.join(aggregate_functions)}"
            
            modified_select = f"SELECT {new_select_content}"
        
        # Reconstruct the query properly
        result_parts = []
        
        # Add CTEs (preserve original formatting)
        if query_parts['ctes']:
            for cte in query_parts['ctes']:
                result_parts.append(cte.strip())
        
        # Add main SELECT
        result_parts.append(modified_select.strip())
        
        # Add FROM clause
        if query_parts['from_clause']:
            result_parts.append(query_parts['from_clause'].strip())
        
        # Add WHERE clause
        if query_parts['where_clause']:
            result_parts.append(query_parts['where_clause'].strip())
        
        # Add GROUP BY clause
        result_parts.append(new_group_by.strip())
        
        # Add HAVING clause
        if query_parts['having_clause']:
            result_parts.append(query_parts['having_clause'].strip())
        
        # Add ORDER BY clause (but remove it if it references non-grouped columns)
        if query_parts['order_by_clause']:
            # For now, we'll keep the ORDER BY clause as is
            # In a more sophisticated implementation, we could validate that
            # ORDER BY columns are either in GROUP BY or are aggregate functions
            result_parts.append(query_parts['order_by_clause'].strip())
        
        # Add LIMIT clause
        if query_parts['limit_clause']:
            result_parts.append(query_parts['limit_clause'].strip())
        
        # Add OFFSET clause
        if query_parts.get('offset_clause'):
            result_parts.append(query_parts['offset_clause'].strip())
        
        # Add FETCH clause
        if query_parts.get('fetch_clause'):
            result_parts.append(query_parts['fetch_clause'].strip())
        
        # Join with proper spacing and formatting
        return '\n'.join(result_parts)
    
    def append_where_clause(self, query: str, where_condition: str, use_parentheses: bool = True) -> str:
        """
        Append a WHERE condition to the main SELECT statement of a SQL query.
        If a WHERE clause already exists in the main query, the new condition is appended with AND.
        This method only modifies the main SELECT's WHERE clause, not any CTEs.
        
        Args:
            query: The SQL query string
            where_condition: The WHERE condition to append (without 'WHERE' keyword)
                           e.g., "md5(hash_key) NOT IN ('value1', 'value2')"
            use_parentheses: If True, wraps complex existing conditions in parentheses for safety.
                           Default is True to ensure proper boolean logic precedence.
        
        Returns:
            Modified query with the WHERE condition appended to the main SELECT
            
        Raises:
            ValueError: If the query is invalid or cannot be processed
        """
        # Input validation
        if not query or not query.strip():
            raise ValueError("Query cannot be empty")
        
        if not where_condition or not where_condition.strip():
            raise ValueError("WHERE condition cannot be empty")
        
        # Sanitize the where_condition to remove leading WHERE keyword if present
        where_condition = re.sub(r'^\s*WHERE\s+', '', where_condition.strip(), flags=re.IGNORECASE)
        
        try:
            # Clean and normalize the query
            query = self._clean_query(query)
            
            # Parse the query structure
            query_parts = self._parse_query_structure(query)
            
            # Resolve ambiguous columns in where_condition using SELECT columns
            where_condition = self._resolve_ambiguous_columns(
                query_parts['main_select'], 
                where_condition
            )
            
            # Modify the WHERE clause
            if query_parts['where_clause']:
                # WHERE clause exists - append with AND
                existing_where = query_parts['where_clause'].strip()
                # Remove the 'WHERE' keyword from existing clause
                existing_condition = re.sub(r'^\s*WHERE\s+', '', existing_where, flags=re.IGNORECASE).strip()
                
                # Check if existing condition has OR operators at the top level
                # If so, wrap in parentheses for safety
                if use_parentheses and self._has_top_level_or(existing_condition):
                    existing_condition = f"({existing_condition})"
                
                # Append new condition with AND
                modified_where = f"WHERE {existing_condition} AND ({where_condition})"
            else:
                # No WHERE clause exists - create new one
                modified_where = f"WHERE {where_condition}"
            
            # Reconstruct the query
            result_parts = []
            
            # Add CTEs (preserve original formatting)
            if query_parts['ctes']:
                for cte in query_parts['ctes']:
                    result_parts.append(cte.strip())
            
            # Add main SELECT
            result_parts.append(query_parts['main_select'].strip())
            
            # Add FROM clause
            if query_parts['from_clause']:
                result_parts.append(query_parts['from_clause'].strip())
            
            # Add modified WHERE clause
            result_parts.append(modified_where.strip())
            
            # Add GROUP BY clause
            if query_parts['group_by_clause']:
                result_parts.append(query_parts['group_by_clause'].strip())
            
            # Add HAVING clause
            if query_parts['having_clause']:
                result_parts.append(query_parts['having_clause'].strip())
            
            # Add ORDER BY clause
            if query_parts['order_by_clause']:
                result_parts.append(query_parts['order_by_clause'].strip())
            
            # Add LIMIT clause
            if query_parts['limit_clause']:
                result_parts.append(query_parts['limit_clause'].strip())
            
            # Add OFFSET clause
            if query_parts.get('offset_clause'):
                result_parts.append(query_parts['offset_clause'].strip())
            
            # Add FETCH clause
            if query_parts.get('fetch_clause'):
                result_parts.append(query_parts['fetch_clause'].strip())
            
            # Join with proper spacing and formatting
            return '\n'.join(result_parts)
            
        except Exception as e:
            raise ValueError(f"Error appending WHERE clause: {str(e)}")
    
    def _extract_columns_with_aliases(self, select_clause: str) -> dict:
        """
        Extract columns from SELECT clause and map them to their qualified names.
        Returns a dictionary mapping column names (case-insensitive) to their qualified forms.
        
        Examples:
            "SELECT a.SALE_ID, b.QUANTITY" -> {"sale_id": "a.SALE_ID", "quantity": "b.QUANTITY"}
            "SELECT SALE_ID, QUANTITY" -> {"sale_id": "SALE_ID", "quantity": "QUANTITY"}
        """
        column_map = {}
        
        # Remove SELECT keyword
        select_content = re.sub(r'^\s*SELECT\s+', '', select_clause, flags=re.IGNORECASE | re.DOTALL).strip()
        
        # Handle SELECT *
        if select_content.strip() == '*':
            return column_map
        
        # Split by comma, handling nested parentheses
        columns = self._split_select_columns(select_content)
        
        for col_expr in columns:
            col_expr = col_expr.strip()
            if not col_expr:
                continue
            
            # Extract the actual column reference (before AS alias if present)
            # Remove AS alias - handle both "AS alias" and implicit alias
            col_without_alias = re.sub(r'\s+AS\s+["\']?(\w+)["\']?$', '', col_expr, flags=re.IGNORECASE)
            col_without_alias = col_without_alias.strip()
            
            # Check if column has table alias (e.g., a.SALE_ID, "a"."SALE_ID", a."SALE_ID", "a".SALE_ID)
            # Pattern: table_alias.column_name with optional quotes
            qualified_patterns = [
                r'(["\']?)(\w+)\1\s*\.\s*(["\']?)(\w+)\3',  # "a"."SALE_ID" or a.SALE_ID
            ]
            
            qualified_match = None
            for pattern in qualified_patterns:
                qualified_match = re.search(pattern, col_without_alias, re.IGNORECASE)
                if qualified_match:
                    break
            
            if qualified_match:
                # Column has table alias
                table_alias = qualified_match.group(2)
                column_name = qualified_match.group(4)
                
                # Preserve original format (quoted or unquoted)
                if '"' in col_without_alias or "'" in col_without_alias:
                    qualified_name = f'{table_alias}."{column_name}"'
                else:
                    qualified_name = f"{table_alias}.{column_name}"
                
                # Map both the column name (case-insensitive) and the qualified name
                column_map[column_name.lower()] = qualified_name
                column_map[qualified_name.lower()] = qualified_name
            else:
                # No table alias - extract just the column name
                # Handle quoted identifiers
                quoted_match = re.search(r'["\']?(\w+)["\']?', col_without_alias)
                if quoted_match:
                    column_name = quoted_match.group(1)
                    # Map to itself (no alias)
                    column_map[column_name.lower()] = column_name
        
        return column_map
    
    def _split_select_columns(self, select_content: str) -> List[str]:
        """
        Split SELECT column list by commas, handling nested parentheses and function calls.
        """
        columns = []
        current_col = ""
        paren_depth = 0
        in_quotes = False
        quote_char = None
        i = 0
        
        while i < len(select_content):
            char = select_content[i]
            
            # Handle quoted strings (both single and double quotes)
            if char in ('"', "'") and (i == 0 or select_content[i-1] != '\\'):
                if not in_quotes:
                    in_quotes = True
                    quote_char = char
                elif char == quote_char:
                    in_quotes = False
                    quote_char = None
            
            if not in_quotes:
                if char == '(':
                    paren_depth += 1
                elif char == ')':
                    paren_depth -= 1
                elif char == ',' and paren_depth == 0:
                    # Top-level comma - split here
                    if current_col.strip():
                        columns.append(current_col.strip())
                    current_col = ""
                    i += 1
                    continue
            
            current_col += char
            i += 1
        
        # Add the last column
        if current_col.strip():
            columns.append(current_col.strip())
        
        return columns
    
    def _resolve_ambiguous_columns(self, select_clause: str, where_condition: str) -> str:
        """
        Resolve ambiguous column names in where_condition by matching them to columns in select_clause.
        If a column in where_condition matches a column in select_clause that has a table alias,
        replace it with the qualified version.
        
        Args:
            select_clause: The SELECT clause from the query
            where_condition: The WHERE condition that may contain ambiguous column names
        
        Returns:
            WHERE condition with resolved column names
        """
        # Extract column mappings from SELECT clause
        column_map = self._extract_columns_with_aliases(select_clause)
        
        if not column_map:
            # No columns found or SELECT * - return as-is
            return where_condition
        
        # Build a list of columns that have table aliases (need resolution)
        aliased_columns = {k: v for k, v in column_map.items() if '.' in v}
        
        if not aliased_columns:
            # No aliased columns in SELECT - return as-is
            return where_condition
        
        # Replace column references in where_condition
        result = where_condition
        
        # Sort by length (longest first) to avoid partial matches
        sorted_columns = sorted(aliased_columns.items(), key=lambda x: len(x[0]), reverse=True)
        
        for column_name, qualified_name in sorted_columns:
            # Pattern 1: Replace quoted column names: "COLUMN_NAME" or 'COLUMN_NAME'
            # Case-insensitive match
            pattern1 = r'(["\'])(' + re.escape(column_name) + r')\1'
            result = re.sub(
                pattern1,
                qualified_name,
                result,
                flags=re.IGNORECASE
            )
            
            # Pattern 2: Replace unquoted column names that are standalone identifiers
            # Use word boundaries, but ensure we don't match if already qualified
            pattern2 = r'\b(' + re.escape(column_name) + r')\b(?![.])'
            
            def replace_func(match):
                matched_text = match.group(0)
                # Check if this is already part of a qualified name (has a dot before it)
                start_pos = match.start()
                if start_pos > 0:
                    before = result[:start_pos]
                    # Check if there's a word followed by a dot before this
                    if re.search(r'\w\s*\.\s*$', before):
                        # Already qualified, don't replace
                        return matched_text
                # Replace with qualified name
                return qualified_name
            
            result = re.sub(
                pattern2,
                replace_func,
                result,
                flags=re.IGNORECASE
            )
        
        return result
    
    def _has_top_level_or(self, condition: str) -> bool:
        """
        Check if the condition has OR operators at the top level (not inside parentheses).
        This helps determine if we need to wrap the condition in parentheses.
        """
        condition_upper = condition.upper()
        paren_depth = 0
        i = 0
        
        while i < len(condition):
            char = condition[i]
            
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            elif paren_depth == 0:
                # We're at the top level, check for OR
                if i + 3 <= len(condition) and condition_upper[i:i+3] == ' OR':
                    # Make sure it's a word boundary (not part of another word)
                    if (i == 0 or not condition[i-1].isalnum()) and \
                       (i + 3 >= len(condition) or not condition[i+3].isalnum()):
                        return True
                elif condition_upper[i:i+4] == ' OR ':
                    return True
            
            i += 1
        
        return False
    
    def append_columns_to_query(self, query: str, 
                                cte_columns: Optional[List[str]] = None,
                                main_select_columns: Optional[List[str]] = None) -> str:
        """
        Append columns to the main SELECT statement (final output).
        This method intelligently handles complex queries including multiple CTEs, joins, 
        and subqueries across all database connectors.
        
        Args:
            query: The SQL query string (simple query, joins, or CTEs)
            cte_columns: List of column expressions to append to the main SELECT clause
                        e.g., ["CONVERT(VARCHAR(32),HASHBYTES('MD5',...)) as hash_key", 
                               "'f7121a6d-328a-4958-990c-3d0eab8c480b' AS user_id"]
                        For CTE queries, these are added to the final SELECT (not to CTEs)
                        For simple queries, these are added to the SELECT
            main_select_columns: List of column names/expressions to append to main SELECT
                                e.g., ["hash_key", "user_id"]
                                Both cte_columns and main_select_columns are combined and
                                added to the final SELECT statement
        
        Returns:
            Modified query with columns appended to the main SELECT statement
            
        Raises:
            ValueError: If the query is invalid or cannot be processed
            
        Examples:
            >>> processor = SQLGroupByProcessor()
            >>> query = "SELECT id, name FROM users"
            >>> cte_cols = ["MD5(name) as hash"]
            >>> main_cols = ["hash"]
            >>> result = processor.append_columns_to_query(query, cte_cols, main_cols)
        """
        # Input validation
        if not query or not query.strip():
            raise ValueError("Query cannot be empty")
        
        if not cte_columns and not main_select_columns:
            # Nothing to append, return original query
            return query
        
        if cte_columns is not None and not isinstance(cte_columns, list):
            raise ValueError("cte_columns must be a list")
        
        if main_select_columns is not None and not isinstance(main_select_columns, list):
            raise ValueError("main_select_columns must be a list")
        
        try:
            # Clean and normalize the query
            query = self._clean_query(query)
            
            # Detect if query has CTEs
            query_upper = query.upper()
            has_cte = 'WITH ' in query_upper
            
            if has_cte:
                # Handle CTE query
                return self._append_columns_to_cte_query(query, cte_columns, main_select_columns)
            else:
                # Handle simple query or query with joins (no CTE)
                return self._append_columns_to_simple_query(query, cte_columns, main_select_columns)
            
        except Exception as e:
            raise ValueError(f"Error appending columns to query: {str(e)}")
    
    def _append_columns_to_simple_query(self, query: str, 
                                       cte_columns: Optional[List[str]],
                                       main_select_columns: Optional[List[str]]) -> str:
        """
        Append columns to a simple query (no CTEs).
        For simple queries, cte_columns are treated as columns to add to the SELECT.
        """
        query_parts = self._parse_query_structure(query)
        
        # Get the main SELECT clause
        main_select = query_parts['main_select']
        
        # Combine columns to append
        columns_to_append = []
        if cte_columns:
            columns_to_append.extend(cte_columns)
        if main_select_columns:
            columns_to_append.extend(main_select_columns)
        
        if not columns_to_append:
            return query
        
        # Modify the SELECT clause to add columns
        modified_select = self._append_columns_to_select_clause(main_select, columns_to_append)
        
        # Reconstruct the query
        result_parts = []
        result_parts.append(modified_select.strip())
        
        if query_parts['from_clause']:
            result_parts.append(query_parts['from_clause'].strip())
        if query_parts['where_clause']:
            result_parts.append(query_parts['where_clause'].strip())
        if query_parts['group_by_clause']:
            result_parts.append(query_parts['group_by_clause'].strip())
        if query_parts['having_clause']:
            result_parts.append(query_parts['having_clause'].strip())
        if query_parts['order_by_clause']:
            result_parts.append(query_parts['order_by_clause'].strip())
        if query_parts['limit_clause']:
            result_parts.append(query_parts['limit_clause'].strip())
        if query_parts.get('offset_clause'):
            result_parts.append(query_parts['offset_clause'].strip())
        if query_parts.get('fetch_clause'):
            result_parts.append(query_parts['fetch_clause'].strip())
        
        return '\n'.join(result_parts)
    
    def _append_columns_to_cte_query(self, query: str,
                                    cte_columns: Optional[List[str]],
                                    main_select_columns: Optional[List[str]]) -> str:
        """
        Append columns to a query with CTEs.
        - cte_columns are added to the main SELECT (final output)
        - main_select_columns are added to the main SELECT
        - Both are combined and added to the final SELECT statement
        """
        query_upper = query.upper()
        
        # Find the WITH keyword position
        with_pos = query_upper.find('WITH ')
        if with_pos == -1:
            raise ValueError("CTE query must contain WITH clause")
        
        # Parse all CTEs (keep them unchanged)
        ctes = self._parse_all_ctes(query[with_pos:])
        
        # Find the main SELECT position (after all CTEs)
        main_select_pos = self._find_main_select_after_ctes(query[with_pos:])
        main_query = query[with_pos + main_select_pos:]
        
        # Combine cte_columns and main_select_columns to add to main SELECT
        columns_to_add_to_main = []
        if cte_columns:
            columns_to_add_to_main.extend(cte_columns)
        if main_select_columns:
            columns_to_add_to_main.extend(main_select_columns)
        
        # Modify main SELECT if we have columns to add
        if columns_to_add_to_main:
            main_query_parts = self._parse_query_structure(main_query)
            modified_main_select = self._append_columns_to_select_clause(
                main_query_parts['main_select'], 
                columns_to_add_to_main
            )
            
            # Reconstruct main query
            main_result_parts = [modified_main_select.strip()]
            if main_query_parts['from_clause']:
                main_result_parts.append(main_query_parts['from_clause'].strip())
            if main_query_parts['where_clause']:
                main_result_parts.append(main_query_parts['where_clause'].strip())
            if main_query_parts['group_by_clause']:
                main_result_parts.append(main_query_parts['group_by_clause'].strip())
            if main_query_parts['having_clause']:
                main_result_parts.append(main_query_parts['having_clause'].strip())
            if main_query_parts['order_by_clause']:
                main_result_parts.append(main_query_parts['order_by_clause'].strip())
            if main_query_parts['limit_clause']:
                main_result_parts.append(main_query_parts['limit_clause'].strip())
            if main_query_parts.get('offset_clause'):
                main_result_parts.append(main_query_parts['offset_clause'].strip())
            if main_query_parts.get('fetch_clause'):
                main_result_parts.append(main_query_parts['fetch_clause'].strip())
            
            main_query = '\n'.join(main_result_parts)
        
        # Combine CTEs and main query
        result = 'WITH ' + ',\n'.join(ctes) + '\n' + main_query
        return result
    
    def _parse_all_ctes(self, query_from_with: str) -> List[str]:
        """
        Parse all CTEs from a query starting with WITH.
        Returns a list of CTE strings (including "CTE_NAME AS (...)").
        """
        query_upper = query_from_with.upper()
        
        # Skip the WITH keyword
        query_after_with = query_from_with[5:].strip()
        query_upper_after_with = query_upper[5:].strip()
        
        ctes = []
        current_pos = 0
        
        while current_pos < len(query_after_with):
            # Find the next CTE name (identifier followed by AS)
            as_match = re.search(r'\bAS\s*\(', query_upper_after_with[current_pos:], re.IGNORECASE)
            if not as_match:
                break
            
            # Find the CTE name (backtrack from AS)
            as_pos = current_pos + as_match.start()
            cte_name_start = current_pos
            
            # Find where this CTE starts (beginning or after a comma)
            # Look backwards for the CTE name
            before_as = query_after_with[current_pos:as_pos].strip()
            cte_name_match = re.search(r'(\w+)\s*$', before_as)
            if cte_name_match:
                cte_name = cte_name_match.group(1)
            else:
                break
            
            # Find the opening parenthesis after AS
            open_paren_pos = as_pos + len('AS')
            while open_paren_pos < len(query_after_with) and query_after_with[open_paren_pos] != '(':
                open_paren_pos += 1
            
            if open_paren_pos >= len(query_after_with):
                break
            
            # Find the matching closing parenthesis
            close_paren_pos = self._find_matching_closing_paren(query_after_with, open_paren_pos)
            
            if close_paren_pos == -1:
                break
            
            # Extract the full CTE (including name and AS (...))
            cte_full = query_after_with[current_pos:close_paren_pos + 1].strip()
            
            # Remove leading comma if present
            cte_full = re.sub(r'^\s*,\s*', '', cte_full)
            
            ctes.append(cte_full)
            
            # Move to position after the closing parenthesis
            current_pos = close_paren_pos + 1
            
            # Check if there's a comma (more CTEs) or main SELECT
            remaining = query_upper_after_with[current_pos:].strip()
            if remaining.startswith(','):
                current_pos += remaining.index(',') + 1
            elif remaining.startswith('SELECT'):
                # Main SELECT found, stop parsing CTEs
                break
            else:
                # Try to find the next SELECT
                select_pos = remaining.find('SELECT')
                if select_pos != -1 and select_pos < remaining.find(',') if ',' in remaining else float('inf'):
                    # Main SELECT is closer than next comma, stop
                    break
                elif ',' in remaining:
                    current_pos += remaining.index(',') + 1
                else:
                    break
        
        return ctes
    
    def _find_matching_closing_paren(self, text: str, open_pos: int) -> int:
        """
        Find the position of the closing parenthesis that matches the opening
        parenthesis at open_pos.
        """
        if text[open_pos] != '(':
            return -1
        
        depth = 1
        pos = open_pos + 1
        
        while pos < len(text) and depth > 0:
            if text[pos] == '(':
                depth += 1
            elif text[pos] == ')':
                depth -= 1
            pos += 1
        
        if depth == 0:
            return pos - 1
        return -1
    
    def _find_main_select_after_ctes(self, query_from_with: str) -> int:
        """
        Find the position of the main SELECT statement after all CTEs.
        """
        query_upper = query_from_with.upper()
        
        # Skip the WITH keyword
        current_pos = 5
        
        # Find all CTE boundaries
        while current_pos < len(query_from_with):
            # Look for AS ( pattern
            as_match = re.search(r'\bAS\s*\(', query_upper[current_pos:], re.IGNORECASE)
            if not as_match:
                break
            
            as_pos = current_pos + as_match.start()
            
            # Find the opening parenthesis
            open_paren_pos = as_pos + len('AS')
            while open_paren_pos < len(query_from_with) and query_from_with[open_paren_pos] != '(':
                open_paren_pos += 1
            
            if open_paren_pos >= len(query_from_with):
                break
            
            # Find matching closing parenthesis
            close_paren_pos = self._find_matching_closing_paren(query_from_with, open_paren_pos)
            
            if close_paren_pos == -1:
                break
            
            # Move past the closing parenthesis
            current_pos = close_paren_pos + 1
            
            # Check what comes next
            remaining = query_upper[current_pos:].lstrip()
            if remaining.startswith(','):
                # Another CTE follows
                current_pos = query_upper.index(',', current_pos) + 1
            elif remaining.startswith('SELECT'):
                # Main SELECT found
                return current_pos + len(query_upper[current_pos:]) - len(remaining)
            else:
                # Look for SELECT
                select_match = re.search(r'\bSELECT\b', remaining)
                if select_match:
                    return current_pos + len(query_upper[current_pos:]) - len(remaining) + select_match.start()
                break
        
        return current_pos
    
    def _append_columns_to_cte(self, cte: str, columns: List[str]) -> str:
        """
        Append columns to a single CTE definition.
        Handles CTEs with nested subqueries, complex expressions, and all SQL clauses.
        CTE format: "CTE_NAME AS (SELECT ... FROM ...)"
        """
        # Find the AS ( position
        cte_upper = cte.upper()
        as_match = re.search(r'\bAS\s*\(', cte_upper, re.IGNORECASE)
        if not as_match:
            raise ValueError("Invalid CTE format: AS keyword not found")
        
        as_pos = as_match.start()
        
        # Extract CTE name
        cte_name = cte[:as_pos].strip()
        
        # Find the opening parenthesis after AS
        open_paren_pos = as_pos + len('AS')
        while open_paren_pos < len(cte) and cte[open_paren_pos] != '(':
            open_paren_pos += 1
        
        # Find the matching closing parenthesis
        close_paren_pos = self._find_matching_closing_paren(cte, open_paren_pos)
        
        if close_paren_pos == -1:
            raise ValueError("Invalid CTE format: Unmatched parentheses")
        
        # Extract the query inside the CTE
        inner_query = cte[open_paren_pos + 1:close_paren_pos].strip()
        
        # Find the top-level SELECT (not nested in subqueries)
        # We need to find the SELECT at parentheses depth 0 relative to the CTE
        inner_query_upper = inner_query.upper()
        
        # Find all SELECT keywords
        select_positions = []
        for match in re.finditer(r'\bSELECT\b', inner_query_upper):
            select_positions.append(match.start())
        
        if not select_positions:
            raise ValueError("No SELECT found in CTE")
        
        # Find the top-level SELECT by checking parentheses depth
        # The top-level SELECT should have balanced parentheses before it
        top_level_select_pos = None
        for select_pos in select_positions:
            # Count parentheses before this SELECT
            before_select = inner_query[:select_pos]
            open_count = before_select.count('(')
            close_count = before_select.count(')')
            # If parentheses are balanced (depth 0), this is the top-level SELECT
            if open_count == close_count:
                top_level_select_pos = select_pos
                break
        
        # If no balanced SELECT found, use the first one (fallback for simple queries)
        if top_level_select_pos is None:
            top_level_select_pos = select_positions[0]
        
        # Calculate the initial parentheses depth at the SELECT position
        before_select = inner_query[:top_level_select_pos]
        initial_depth = before_select.count('(') - before_select.count(')')
        
        # Start from after the SELECT keyword
        select_end = top_level_select_pos + len('SELECT')
        
        # Find FROM at the same depth level as the top-level SELECT
        from_pos = self._find_keyword_at_depth_level(inner_query, 'FROM', select_end, initial_depth)
        
        # If no FROM found, try to find other clauses to determine insertion point
        if from_pos == -1:
            # Try to find WHERE, GROUP BY, ORDER BY, etc. at the same depth
            where_pos = self._find_keyword_at_depth_level(inner_query, 'WHERE', select_end, initial_depth)
            group_by_pos = self._find_keyword_at_depth_level(inner_query, 'GROUP BY', select_end, initial_depth)
            having_pos = self._find_keyword_at_depth_level(inner_query, 'HAVING', select_end, initial_depth)
            order_by_pos = self._find_keyword_at_depth_level(inner_query, 'ORDER BY', select_end, initial_depth)
            
            # Find the first clause after SELECT at the same depth
            next_clause_pos = len(inner_query)
            for pos in [where_pos, group_by_pos, having_pos, order_by_pos]:
                if pos != -1 and pos < next_clause_pos:
                    next_clause_pos = pos
            
            if next_clause_pos < len(inner_query):
                # Insert before the next clause
                select_part = inner_query[top_level_select_pos:next_clause_pos].strip()
                rest_part = inner_query[next_clause_pos:].strip()
                
                # Find the end of the column list by removing SELECT keyword
                select_keyword_match = re.match(r'(\s*SELECT\s+)(.*)', select_part, re.IGNORECASE | re.DOTALL)
                if select_keyword_match:
                    select_keyword = select_keyword_match.group(1)
                    column_list = select_keyword_match.group(2).strip()
                    
                    # Append columns at the end of the column list
                    columns_str = ', '.join(columns)
                    new_column_list = f"{column_list}, {columns_str}"
                    modified_select = f"{select_keyword}{new_column_list}"
                else:
                    # Fallback: append to the whole SELECT part
                    columns_str = ', '.join(columns)
                    modified_select = f"{select_part}, {columns_str}"
                
                # Reconstruct the CTE
                modified_inner_query = f"{modified_select}\n    {rest_part}"
                return f"{cte_name} AS (\n    {modified_inner_query}\n)"
            else:
                # No other clauses found, append at the end of SELECT clause
                select_part = inner_query[top_level_select_pos:].strip()
                
                # Find the end of the column list by removing SELECT keyword
                select_keyword_match = re.match(r'(\s*SELECT\s+)(.*)', select_part, re.IGNORECASE | re.DOTALL)
                if select_keyword_match:
                    select_keyword = select_keyword_match.group(1)
                    column_list = select_keyword_match.group(2).strip()
                    
                    # Append columns at the end of the column list
                    columns_str = ', '.join(columns)
                    new_column_list = f"{column_list}, {columns_str}"
                    modified_select = f"{select_keyword}{new_column_list}"
                else:
                    # Fallback: append to the whole SELECT part
                    columns_str = ', '.join(columns)
                    modified_select = f"{select_part}, {columns_str}"
                
                modified_inner_query = modified_select
                return f"{cte_name} AS (\n    {modified_inner_query}\n)"
        
        # Extract the part before FROM (SELECT clause) and after FROM
        select_part = inner_query[top_level_select_pos:from_pos].strip()
        from_and_rest = inner_query[from_pos:].strip()
        
        # Find the end of the column list by looking for the last column
        # We need to append columns at the END of the column list, not at the beginning
        # Remove SELECT keyword to get just the column list
        select_keyword_match = re.match(r'(\s*SELECT\s+)(.*)', select_part, re.IGNORECASE | re.DOTALL)
        if select_keyword_match:
            select_keyword = select_keyword_match.group(1)
            column_list = select_keyword_match.group(2).strip()
            
            # Append columns at the end of the column list
            columns_str = ', '.join(columns)
            new_column_list = f"{column_list}, {columns_str}"
            modified_select = f"{select_keyword}{new_column_list}"
        else:
            # Fallback: append to the whole SELECT part
            columns_str = ', '.join(columns)
            modified_select = f"{select_part}, {columns_str}"
        
        # Reconstruct the CTE
        modified_inner_query = f"{modified_select}\n    {from_and_rest}"
        
        return f"{cte_name} AS (\n    {modified_inner_query}\n)"
    
    def _find_keyword_at_top_level(self, query: str, keyword: str, start_pos: int = 0) -> int:
        """
        Find a SQL keyword at the top level (not inside parentheses).
        Returns the position of the keyword, or -1 if not found.
        """
        query_upper = query.upper()
        paren_depth = 0
        pos = start_pos
        
        while pos < len(query):
            char = query[pos]
            
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            elif paren_depth == 0:
                # We're at the top level, check for keyword
                remaining = query_upper[pos:]
                if remaining.startswith(keyword):
                    # Check word boundary
                    if pos == 0 or not query[pos-1].isalnum():
                        end_pos = pos + len(keyword)
                        if end_pos >= len(query) or not query[end_pos].isalnum():
                            return pos
            
            pos += 1
        
        return -1
    
    def _find_keyword_at_depth_level(self, query: str, keyword: str, start_pos: int = 0, target_depth: int = 0) -> int:
        """
        Find a SQL keyword at a specific parentheses depth level.
        Returns the position of the keyword, or -1 if not found.
        
        This method is used to find keywords at the same depth as a specific SELECT statement,
        which is useful for CTEs with nested subqueries.
        
        Args:
            query: The query string to search
            keyword: The keyword to find (e.g., 'FROM', 'WHERE')
            start_pos: Position to start searching from
            target_depth: The target parentheses depth (0 = top level)
        """
        query_upper = query.upper()
        pos = start_pos
        
        # Calculate initial depth at start_pos
        before_start = query[:start_pos]
        initial_open = before_start.count('(')
        initial_close = before_start.count(')')
        initial_depth = initial_open - initial_close
        
        # We want to find keywords at the same level as where we started
        # So target_depth should be relative to initial_depth
        paren_depth = initial_depth
        
        while pos < len(query):
            char = query[pos]
            
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            elif paren_depth == target_depth:
                # We're at the target depth, check for keyword
                remaining = query_upper[pos:]
                if remaining.startswith(keyword):
                    # Check word boundary
                    if pos == 0 or not query[pos-1].isalnum():
                        end_pos = pos + len(keyword)
                        if end_pos >= len(query) or not query[end_pos].isalnum():
                            return pos
            
            pos += 1
        
        return -1
    
    def _append_columns_to_select_clause(self, select_clause: str, columns: List[str]) -> str:
        """
        Append columns to a SELECT clause.
        
        Args:
            select_clause: The SELECT clause (e.g., "SELECT col1, col2")
            columns: List of column expressions to append
        
        Returns:
            Modified SELECT clause with columns appended
        """
        # Extract the SELECT keyword and column list
        select_match = re.match(r'(\s*SELECT\s+)(.*)', select_clause, re.IGNORECASE | re.DOTALL)
        if not select_match:
            raise ValueError("Invalid SELECT clause format")
        
        select_keyword = select_match.group(1)
        column_list = select_match.group(2).strip()
        
        # Append the new columns
        if columns:
            columns_str = ', '.join(columns)
            new_column_list = f"{column_list}, {columns_str}"
        else:
            new_column_list = column_list
        
        return f"{select_keyword}{new_column_list}"
    
    def get_available_columns(self, query: str) -> List[str]:
        """
        Extract available columns from the query for GROUP BY reference.
        This is a helper method to identify columns that can be used for grouping.
        """
        try:
            query_parts = self._parse_query_structure(self._clean_query(query))
            
            # Extract columns from SELECT clause
            select_clause = query_parts['main_select']
            
            # Remove SELECT keyword
            select_content = re.sub(r'^\s*SELECT\s+', '', select_clause, flags=re.IGNORECASE)
            
            # If SELECT * is used, we can't determine specific columns
            if re.search(r'\b\*\b', select_content, re.IGNORECASE):
                return ["* (SELECT * used - specific columns not determinable)"]
            
            # Extract column names (simple approach)
            columns = []
            # Split by comma and clean up
            column_parts = [part.strip() for part in select_content.split(',')]
            
            for part in column_parts:
                # Remove aliases (AS keyword)
                column = re.sub(r'\s+AS\s+\w+$', '', part, flags=re.IGNORECASE)
                # Extract column name (before any function calls)
                column_match = re.search(r'(\w+(?:\.\w+)?)', column)
                if column_match:
                    columns.append(column_match.group(1))
            
            return columns
            
        except Exception as e:
            return [f"Error extracting columns: {str(e)}"]


def convert_query_to_group_by(query: str, group_by_column: str, 
                           aggregate_functions: Optional[List[str]] = None) -> str:
    """
    Convenience function to apply GROUP BY clause to a SQL query.
    
    Args:
        query: The SQL query string
        group_by_column: The column name to group by
        aggregate_functions: Optional list of aggregate functions to apply
    
    Returns:
        Modified query with GROUP BY clause applied
    """
    processor = SQLGroupByProcessor()
    return processor.apply_group_by(query, group_by_column, aggregate_functions)


def append_where_to_query(query: str, where_condition: str, use_parentheses: bool = True) -> str:
    """
    Convenience function to append a WHERE condition to the main SELECT of a SQL query.
    This function intelligently handles complex queries including CTEs, subqueries, and nested conditions.
    
    Args:
        query: The SQL query string (can be simple or complex with CTEs)
        where_condition: The WHERE condition to append (without 'WHERE' keyword)
                        e.g., "md5(hash_key) NOT IN ('value1', 'value2')"
        use_parentheses: If True, wraps conditions in parentheses for proper boolean logic.
                        Default is True for safety. Set to False only if you're certain about precedence.
    
    Returns:
        Modified query with WHERE condition appended to the main SELECT only (not CTEs)
    
    Note:
        - This function only modifies the main SELECT statement's WHERE clause
        - CTEs and their WHERE clauses remain unchanged
        - Automatically handles parentheses balancing for nested subqueries
        - Safely wraps OR conditions to maintain proper boolean logic precedence
    """
    processor = SQLGroupByProcessor()
    return processor.append_where_clause(query, where_condition, use_parentheses)


def append_columns_to_query(query: str, 
                           cte_columns: Optional[List[str]] = None,
                           main_select_columns: Optional[List[str]] = None) -> str:
    """
    Convenience function to append columns to the main SELECT statement (final output).
    This function intelligently handles simple queries, joins, and complex CTEs across all database connectors.
    
    Args:
        query: The SQL query string (simple query, joins, or CTEs)
        cte_columns: List of column expressions to append to the main SELECT clause
                For CTE queries, these are added to the final SELECT (not to CTEs)
                For simple queries, these are added to the SELECT
        main_select_columns: List of column names/expressions to append to main SELECT
                            e.g., ["hash_key", "user_id"]
                            Both cte_columns and main_select_columns are combined and
                            added to the final SELECT statement
    
    Returns:
        Modified query with columns appended to the main SELECT statement
    
    Note:
        - For CTE queries: cte_columns and main_select_columns are both added to the final SELECT
        - For simple queries: both cte_columns and main_select_columns are added to the SELECT
        - Supports multiple CTEs, joins, subqueries across all SQL dialects
        - Preserves query structure and formatting
        - CTEs remain unchanged; only the final SELECT is modified
    """
    processor = SQLGroupByProcessor()
    return processor.append_columns_to_query(query, cte_columns, main_select_columns)