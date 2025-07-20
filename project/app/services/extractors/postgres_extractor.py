from typing import Dict, Any, List, Optional
import asyncpg

class PostgresExtractor:
    """
    Extract metadata from PostgreSQL databases.
    """
    
    def __init__(self, connection_params: Dict[str, Any]):
        """
        Initialize the extractor with connection parameters.
        """
        self.host = connection_params.get("host", "localhost")
        # self.host = "postgres-backend"
        self.port = connection_params.get("port", 5432)
        self.database = connection_params.get("database")
        self.username = connection_params.get("username")
        self.password = connection_params.get("password")
        self.ssl = connection_params.get("ssl", False)
        self.connection = None
        
    async def _get_connection(self):
        """
        Get or create a connection to PostgreSQL.
        """
        if not self.connection:
            self.connection = await asyncpg.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password,
                ssl=self.ssl
            )
        return self.connection
    
    async def _close_connection(self):
        """
        Close the PostgreSQL connection.
        """
        if self.connection:
            await self.connection.close()
            self.connection = None
    
    async def extract_catalogs(self) -> List[Dict[str, Any]]:
        """
        PostgreSQL doesn't have catalogs in the same way as other databases.
        We'll return just one catalog representing the database.
        """
        return [{
            "catalog_name": self.database,
            "catalog_type": "postgresql",
            "external_reference": self.database,
            "properties": {}
        }]
    
    async def extract_schemas(self) -> List[Dict[str, Any]]:
        """
        Extract schemas from PostgreSQL database.
        """
        try:
            conn = await self._get_connection()
            
            query = """
            SELECT 
                schema_name,
                schema_owner AS owner
            FROM 
                information_schema.schemata
            WHERE 
                schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
            ORDER BY 
                schema_name
            """
            
            rows = await conn.fetch(query)
            
            schemas = []
            for row in rows:
                schemas.append({
                    "schema_name": row["schema_name"],
                    "catalog_name": self.database,
                    "external_reference": f"{self.database}.{row['schema_name']}",
                    "properties": {
                        "owner": row["owner"]
                    }
                })
            
            return schemas
            
        except Exception as e:
            print(f"Error extracting schemas: {str(e)}")
            return []
    
    async def extract_tables(self, schema_name: str) -> List[Dict[str, Any]]:
        """
        Extract tables from a PostgreSQL schema.
        """
        try:
            conn = await self._get_connection()
            
            query = """
            SELECT 
                table_name,
                table_type,
                pg_class.reltuples::bigint AS estimated_row_count,
                pg_total_relation_size(quote_ident(table_schema) || '.' || quote_ident(table_name))::bigint AS total_size_bytes,
                obj_description(pg_class.oid) AS description
            FROM 
                information_schema.tables
            JOIN 
                pg_namespace ON pg_namespace.nspname = table_schema
            JOIN 
                pg_class ON pg_class.relnamespace = pg_namespace.oid
                AND pg_class.relname = table_name
            WHERE 
                table_schema = $1
            ORDER BY 
                table_name
            """
            
            rows = await conn.fetch(query, schema_name)
            
            tables = []
            for row in rows:
                table_type = "table"
                if row["table_type"] == "VIEW":
                    table_type = "view"
                elif row["table_type"] == "FOREIGN TABLE":
                    table_type = "foreign_table"
                
                tables.append({
                    "table_name": row["table_name"],
                    "external_reference": f"{self.database}.{schema_name}.{row['table_name']}",
                    "table_type": table_type,
                    "estimated_row_count": row["estimated_row_count"],
                    "total_size_bytes": row["total_size_bytes"],
                    "last_analyzed": None,  # PostgreSQL doesn't store this explicitly
                    "description": row["description"],
                    "properties": {
                        "original_table_type": row["table_type"]
                    }
                })
            
            return tables
            
        except Exception as e:
            print(f"Error extracting tables for schema {schema_name}: {str(e)}")
            return []
    
    async def extract_columns(self, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """
        Extract columns for a specific table.
        """
        try:
            conn = await self._get_connection()
            
            # Get column information
            column_query = """
            SELECT 
                c.column_name,
                c.data_type,
                c.is_nullable = 'YES' AS is_nullable,
                c.ordinal_position AS column_position,
                c.character_maximum_length AS max_length,
                c.numeric_precision,
                c.numeric_scale,
                c.column_default AS default_value,
                pg_catalog.col_description(format('%s.%s', c.table_schema, c.table_name)::regclass::oid, c.ordinal_position) AS description,
                format_type(a.atttypid, a.atttypmod) AS full_data_type
            FROM 
                information_schema.columns c
            JOIN 
                pg_namespace n ON n.nspname = c.table_schema
            JOIN 
                pg_class cl ON cl.relnamespace = n.oid AND cl.relname = c.table_name
            JOIN 
                pg_attribute a ON a.attrelid = cl.oid AND a.attname = c.column_name AND a.attnum > 0
            WHERE 
                c.table_schema = $1
                AND c.table_name = $2
            ORDER BY 
                c.ordinal_position
            """
            
            column_rows = await conn.fetch(column_query, schema_name, table_name)

            print("DEBUG: Column Rows:", column_rows)
            
            # Get primary key information
            pk_query = """
            SELECT 
                c.column_name
            FROM 
                information_schema.table_constraints tc
            JOIN 
                information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
                AND tc.table_schema = ccu.table_schema
            JOIN 
                information_schema.columns c ON c.table_schema = tc.table_schema
                AND c.table_name = tc.table_name
                AND c.column_name = ccu.column_name
            WHERE 
                tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = $1
                AND tc.table_name = $2
            """
            
            pk_rows = await conn.fetch(pk_query, schema_name, table_name)
            pk_columns = set(row["column_name"] for row in pk_rows)

            print("DEBUG: Primary Key Columns:", pk_columns)
            
            # Get unique constraint information
            unique_query = """
            SELECT 
                c.column_name
            FROM 
                information_schema.table_constraints tc
            JOIN 
                information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
                AND tc.table_schema = ccu.table_schema
            JOIN 
                information_schema.columns c ON c.table_schema = tc.table_schema
                AND c.table_name = tc.table_name
                AND c.column_name = ccu.column_name
            WHERE 
                tc.constraint_type = 'UNIQUE'
                AND tc.table_schema = $1
                AND tc.table_name = $2
            """
            
            unique_rows = await conn.fetch(unique_query, schema_name, table_name)
            unique_columns = set(row["column_name"] for row in unique_rows)

            print("DEBUG: Unique Columns:", unique_columns)
            
            # Get indexed columns
            index_query = """
            SELECT 
                a.attname AS column_name
            FROM 
                pg_index i
            JOIN 
                pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            JOIN 
                pg_class t ON t.oid = i.indrelid
            JOIN 
                pg_namespace n ON n.oid = t.relnamespace
            WHERE 
                n.nspname = $1
                AND t.relname = $2
            """

            index_rows = await conn.fetch(index_query, schema_name, table_name)
            indexed_columns = set(row["column_name"] for row in index_rows)

            print("DEBUG: Indexed Columns:", indexed_columns)
            
            columns = []
            for row in column_rows:
                column_name = row["column_name"]
                
                # Get basic statistics
                stats_query = """
                SELECT
                    n_distinct,
                    most_common_vals::text,
                    most_common_freqs::text,
                    histogram_bounds::text
                FROM
                    pg_stats
                WHERE
                    schemaname = $1
                    AND tablename = $2
                    AND attname = $3
                """
                
                stats = await conn.fetchrow(stats_query, schema_name, table_name, column_name)
                
                statistics = {}
                if stats:
                    if stats["n_distinct"] is not None:
                        if stats["n_distinct"] > 0:
                            statistics["distinct_values"] = int(stats["n_distinct"])
                        else:
                            # Negative values are ratios of the total number of rows
                            statistics["distinct_values_ratio"] = -float(stats["n_distinct"])
                    
                    if stats["most_common_vals"]:
                        statistics["most_common_values"] = stats["most_common_vals"]
                        statistics["most_common_freqs"] = stats["most_common_freqs"]
                    
                    if stats["histogram_bounds"]:
                        statistics["histogram_bounds"] = stats["histogram_bounds"]
                
                # Get sample values 
                sample_query = f"""
                SELECT {column_name}
                FROM {schema_name}.{table_name}
                ORDER BY random()
                LIMIT 5
                """
                
                try:
                    sample_rows = await conn.fetch(sample_query)
                    sample_values = [row[0] for row in sample_rows]
                    # Convert any complex types to string
                    sample_values = [str(val) if val is not None else None for val in sample_values]
                except:
                    sample_values = []
                
                columns.append({
                    "column_name": column_name,
                    "external_reference": f"{self.database}.{schema_name}.{table_name}.{column_name}",
                    "data_type": row["data_type"],
                    "is_nullable": row["is_nullable"],
                    "column_position": row["column_position"],
                    "max_length": row["max_length"],
                    "numeric_precision": row["numeric_precision"],
                    "numeric_scale": row["numeric_scale"],
                    "is_primary_key": column_name in pk_columns,
                    "is_unique": column_name in unique_columns,
                    "is_indexed": column_name in indexed_columns,
                    "default_value": row["default_value"],
                    "description": row["description"],
                    "statistics": statistics,
                    "sample_values": sample_values,
                    "properties": {
                        "full_data_type": row["full_data_type"]
                    }
                })
            
            return columns
            
        except Exception as e:
            print(f"Error extracting columns for table {schema_name}.{table_name}: {str(e)}")
            return []
    
    async def extract_table_data(self, schema_name: str, table_name: str, limit: Optional[int] = None, offset: int = 0) -> Dict[str, Any]:
        """
        Extract actual data from a PostgreSQL table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            limit: Maximum number of rows to extract (None for all)
            offset: Number of rows to skip
        
        Returns:
            Dict containing data, columns info, and row count
        """
        try:
            conn = await self._get_connection()
            
            # Get column information first
            columns = await self.extract_columns(schema_name, table_name)
            
            # Build query
            query = f'SELECT * FROM "{schema_name}"."{table_name}"'
            
            # Add LIMIT and OFFSET if specified
            if limit is not None:
                query += f" LIMIT {limit}"
            if offset > 0:
                query += f" OFFSET {offset}"
            
            # Execute query
            rows = await conn.fetch(query)
            
            # Convert rows to list of dictionaries
            data = []
            for row in rows:
                row_dict = {}
                for col in columns:
                    col_name = col['column_name']
                    row_dict[col_name] = row.get(col_name)
                data.append(row_dict)
            
            # Get total row count
            count_query = f'SELECT COUNT(*) as total FROM "{schema_name}"."{table_name}"'
            count_result = await conn.fetchrow(count_query)
            total_rows = count_result['total'] if count_result else 0
            
            return {
                "schema_name": schema_name,
                "table_name": table_name,
                "columns": columns,
                "data": data,
                "total_rows": total_rows,
                "fetched_rows": len(data),
                "offset": offset
            }
            
        except Exception as e:
            print(f"Error extracting data from table {schema_name}.{table_name}: {str(e)}")
            return {
                "schema_name": schema_name,
                "table_name": table_name,
                "columns": [],
                "data": [],
                "total_rows": 0,
                "fetched_rows": 0,
                "offset": offset,
                "error": str(e)
            }
    
    async def extract_table_data_chunked(self, schema_name: str, table_name: str, chunk_size: int = 10000) -> List[Dict[str, Any]]:
        """
        Extract table data in chunks for large tables.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            chunk_size: Number of rows per chunk
        
        Yields:
            Dict containing chunk data
        """
        try:
            # Get total row count first
            conn = await self._get_connection()
            count_query = f'SELECT COUNT(*) as total FROM "{schema_name}"."{table_name}"'
            count_result = await conn.fetchrow(count_query)
            total_rows = count_result['total'] if count_result else 0
            
            chunks = []
            offset = 0
            
            while offset < total_rows:
                chunk_data = await self.extract_table_data(schema_name, table_name, chunk_size, offset)
                chunks.append(chunk_data)
                offset += chunk_size
                
                # Break if no more data
                if len(chunk_data.get('data', [])) < chunk_size:
                    break
            
            return chunks
            
        except Exception as e:
            print(f"Error extracting chunked data from table {schema_name}.{table_name}: {str(e)}")
            return []
    
    async def close(self):
        """
        Close any open connections.
        """
        await self._close_connection()