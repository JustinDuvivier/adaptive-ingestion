class DatabaseSetup:
    def __init__(self, db_connection):
        self.db = db_connection

    def _table_exists(self, table_name: str) -> bool:
        result = self.db.execute_query("""
                                       SELECT EXISTS (SELECT 1
                                                      FROM information_schema.tables
                                                      WHERE table_schema = 'public'
                                                        AND table_name = %s)
                                       """, (table_name,))
        return result[0][0]

    def _constraint_exists(self, table_name: str, constraint_name: str) -> bool:
        result = self.db.execute_query("""
                                       SELECT EXISTS (SELECT 1
                                                      FROM information_schema.table_constraints
                                                      WHERE table_schema = 'public'
                                                        AND table_name = %s
                                                        AND constraint_name = %s)
                                       """, (table_name, constraint_name))
        return result[0][0]

    def _setup_pgvector(self) -> None:
        try:
            self.db.execute_update("CREATE EXTENSION IF NOT EXISTS vector")
            print("✅ pgvector extension enabled")
        except Exception as e:
            if "already exists" in str(e).lower():
                print("📋 pgvector extension already enabled")
            else:
                print(f"⚠️  Could not enable pgvector: {e}")
                print("   Please ensure pgvector is installed on your PostgreSQL server")
                raise

    def _create_analysis_registry(self) -> None:
        if not self._table_exists('analysis_registry'):
            self.db.execute_update("""
                                   CREATE TABLE analysis_registry
                                   (
                                       analysis_id      SERIAL PRIMARY KEY,
                                       session_id       UUID NOT NULL,
                                       dataset_name     VARCHAR(50),
                                       user_interest    TEXT,
                                       table_name       VARCHAR(100),
                                       schema_json      JSONB,
                                       status           VARCHAR(20) DEFAULT 'active'
                                           CHECK (status IN ('active', 'completed', 'failed')),
                                       created_at       TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
                                       updated_at       TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
                                       records_loaded   INTEGER     DEFAULT 0,
                                       records_rejected INTEGER     DEFAULT 0
                                   )
                                   """)
            print("✅ Created analysis_registry table")
        else:
            print("📋 analysis_registry table already exists")

    def _create_stg_rejects(self) -> None:
        if not self._table_exists('stg_rejects'):
            self.db.execute_update("""
                                   CREATE TABLE stg_rejects
                                   (
                                       reject_id   SERIAL PRIMARY KEY,
                                       source_name TEXT  NOT NULL,
                                       table_name  TEXT,
                                       raw_payload JSONB NOT NULL,
                                       reason      TEXT,
                                       rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                                   )
                                   """)
            print("✅ Created stg_rejects table")
        else:
            print("📋 stg_rejects table already exists")

    def _create_insights_cache(self) -> None:
        if not self._table_exists('insights_cache'):
            self.db.execute_update("""
                                   CREATE TABLE insights_cache
                                   (
                                       insight_id       SERIAL PRIMARY KEY,
                                       analysis_id      INTEGER,
                                       insight_type     VARCHAR(50),
                                       title            TEXT,
                                       description      TEXT,
                                       confidence_score DECIMAL(3, 2)
                                           CHECK (confidence_score >= 0 AND confidence_score <= 1),
                                       impact_level     VARCHAR(20)
                                           CHECK (impact_level IN ('critical', 'high', 'medium', 'low')),
                                       metadata         JSONB,
                                       generated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                                   )
                                   """)
            print("✅ Created insights_cache table")
        else:
            print("📋 insights_cache table already exists")

    def _create_query_history(self) -> None:
        if not self._table_exists('query_history'):
            self.db.execute_update("""
                                   CREATE TABLE query_history
                                   (
                                       query_id                  SERIAL PRIMARY KEY,
                                       analysis_id               INTEGER,
                                       natural_language_question TEXT NOT NULL,
                                       generated_sql             TEXT,
                                       result_count              INTEGER,
                                       execution_time_ms         INTEGER,
                                       error_message             TEXT,
                                       created_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                                   )
                                   """)
            print("✅ Created query_history table")
        else:
            print("📋 query_history table already exists")

    def _create_embeddings(self) -> None:
        if not self._table_exists('embeddings'):
            self.db.execute_update("""
                                   CREATE TABLE embeddings
                                   (
                                       embedding_id  SERIAL PRIMARY KEY,
                                       analysis_id   INTEGER,
                                       content_type  VARCHAR(50)
                                           CHECK (content_type IN ('data', 'insight', 'schema', 'query')),
                                       original_text TEXT NOT NULL,
                                       embedding     vector(1536),
                                       metadata      JSONB,
                                       created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                                   )
                                   """)
            print("✅ Created embeddings table")
        else:
            print("📋 embeddings table already exists")

    def _add_foreign_keys(self) -> None:
        constraints = [
            ('insights_cache', 'analysis_id', 'fk_insights_analysis'),
            ('query_history', 'analysis_id', 'fk_query_analysis'),
            ('embeddings', 'analysis_id', 'fk_embeddings_analysis')
        ]

        for table, column, constraint_name in constraints:
            if not self._constraint_exists(table, constraint_name):
                try:
                    self.db.execute_update(f"""
                        ALTER TABLE {table}
                        ADD CONSTRAINT {constraint_name}
                        FOREIGN KEY ({column})
                        REFERENCES analysis_registry(analysis_id)
                        ON DELETE CASCADE
                    """)
                    print(f"✅ Added foreign key: {constraint_name}")
                except Exception as e:
                    if "already exists" in str(e).lower():
                        print(f"📋 Foreign key {constraint_name} already exists")
                    else:
                        raise
            else:
                print(f"📋 Foreign key {constraint_name} already exists")

    def _create_indexes(self) -> None:
        indexes = [
            ('idx_analysis_registry_session', 'analysis_registry', 'session_id'),
            ('idx_analysis_registry_status', 'analysis_registry', 'status'),
            ('idx_stg_rejects_source', 'stg_rejects', 'source_name'),
            ('idx_stg_rejects_timestamp', 'stg_rejects', 'rejected_at'),
            ('idx_insights_cache_analysis', 'insights_cache', 'analysis_id'),
            ('idx_insights_cache_type', 'insights_cache', 'insight_type'),
            ('idx_query_history_analysis', 'query_history', 'analysis_id'),
            ('idx_embeddings_analysis', 'embeddings', 'analysis_id'),
            ('idx_embeddings_type', 'embeddings', 'content_type')
        ]

        for index_name, table_name, column_name in indexes:
            try:
                self.db.execute_update(f"""
                    CREATE INDEX IF NOT EXISTS {index_name} 
                    ON {table_name}({column_name})
                """)
                print(f"✅ Created index: {index_name}")
            except Exception as e:
                if "already exists" in str(e).lower():
                    print(f"📋 Index {index_name} already exists")
                else:
                    print(f"⚠️  Could not create index {index_name}: {e}")

        try:
            self.db.execute_update("""
                                   CREATE INDEX IF NOT EXISTS idx_embeddings_vector
                                       ON embeddings
                                       USING ivfflat (embedding vector_cosine_ops)
                                       WITH (lists = 100)
                                   """)
            print("✅ Created vector similarity index")
        except Exception as e:
            if "already exists" in str(e).lower():
                print("📋 Vector index already exists")
            elif "does not exist" in str(e).lower() and "vector_cosine_ops" in str(e).lower():
                print("⚠️  Could not create vector index - pgvector might not be installed")
            else:
                print(f"⚠️  Could not create vector index: {e}")

    def _drop_all_tables(self) -> None:
        response = input("\n⚠️  WARNING: This will DELETE all tables and data! Type 'yes' to confirm: ")
        if response.lower() != 'yes':
            print("Skipping table drop")
            return

        tables = ['embeddings', 'query_history', 'insights_cache', 'stg_rejects', 'analysis_registry']
        for table in tables:
            try:
                self.db.execute_update(f"DROP TABLE IF EXISTS {table} CASCADE")
                print(f"❌ Dropped table: {table}")
            except Exception as e:
                print(f"Could not drop {table}: {e}")

    def setup_all(self, drop_existing: bool = False) -> None:
        print("\n🚀 Starting database setup...")

        if drop_existing:
            self._drop_all_tables()

        self._setup_pgvector()
        self._create_analysis_registry()
        self._create_stg_rejects()
        self._create_insights_cache()
        self._create_query_history()
        self._create_embeddings()
        self._add_foreign_keys()
        self._create_indexes()

        print("\n✅ Database setup complete!")

    def verify_setup(self) -> bool:
        print("\n🔍 Verifying database setup...")

        required_tables = ['analysis_registry', 'stg_rejects', 'insights_cache', 'query_history', 'embeddings']
        all_good = True

        for table in required_tables:
            if self._table_exists(table):
                try:
                    count = self.db.execute_query(f"SELECT COUNT(*) FROM {table}")
                    print(f"✅ {table}: OK ({count[0][0]} records)")
                except Exception as e:
                    print(f"❌ {table}: EXISTS but cannot query: {e}")
                    all_good = False
            else:
                print(f"❌ {table}: NOT FOUND")
                all_good = False

        try:
            result = self.db.execute_query("""
                                           SELECT EXISTS (SELECT 1
                                                          FROM pg_extension
                                                          WHERE extname = 'vector')
                                           """)
            if result[0][0]:
                print("✅ pgvector: INSTALLED")
            else:
                print("⚠️  pgvector: NOT INSTALLED (embeddings won't work)")
        except Exception as e:
            print(f"⚠️  Could not check pgvector: {e}")

        return all_good