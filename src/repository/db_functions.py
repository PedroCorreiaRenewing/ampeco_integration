from sqlalchemy import create_engine, MetaData, Table, func, select, text
from sqlalchemy.dialects.postgresql import insert
import pandas as pd



class PostgresInteraction:
    def __init__(self, engine):
        self.engine = engine
        self.metadata = MetaData()

    @classmethod
    def from_engine(cls, engine):
        return cls(engine)

    def upsert_dataframe(
        self,
        table_name: str,
        df,
        conflict_columns: list[str],
        exclude_update_columns: list[str] | None = None,
    ):
        if df.empty:
            return

        df = df.where(df.notna(), None)

        table = Table(table_name, self.metadata, autoload_with=self.engine)

        #filter just the columns that exist in the table
        table_columns = set(c.name for c in table.columns)
        df_columns = set(df.columns)

        valid_columns = list(table_columns & df_columns)

        if not valid_columns:
            raise ValueError(
                f"No matching columns between DataFrame and table '{table_name}'"
            )

        df = df[valid_columns]

        records = df.to_dict(orient="records")

        stmt = insert(table).values(records)

        exclude_update_columns = set(exclude_update_columns or [])
        exclude_update_columns.add("id")  #never update PK

        update_columns = {
            c.name: stmt.excluded[c.name]
            for c in table.columns
            if c.name not in exclude_update_columns
        }

        stmt = stmt.on_conflict_do_update(
            index_elements=conflict_columns,
            set_=update_columns
        )

        with self.engine.begin() as conn:
            conn.execute(stmt)

    def fetch(
        self,
        table_name: str,
        columns: list[str] | None = None,
        functions: dict[str, str] | None = None,
        where: dict | None = None,
        distinct: bool = False,
        as_dict: bool = True,
        as_dataframe: bool = False,
    ):
        """
        columns: ["id", "source_id"]
        functions: {"max_power": "max", "id": "count"}
        where: {"source_system_id": 1}
        """

        table = Table(table_name, self.metadata, autoload_with=self.engine)

        select_items = []

        # ---- plain columns ----
        if columns:
            for col in columns:
                if col not in table.c:
                    raise ValueError(f"Column '{col}' not in table '{table_name}'")
                select_items.append(table.c[col])

        # ---- SQL functions ----
        if functions:
            for col, fn in functions.items():
                if col not in table.c:
                    raise ValueError(f"Column '{col}' not in table '{table_name}'")

                if not hasattr(func, fn):
                    raise ValueError(f"SQL function '{fn}' is not supported")

                select_items.append(
                    getattr(func, fn)(table.c[col]).label(f"{fn}_{col}")
                )

        if not select_items:
            raise ValueError("You must provide columns or functions")

        stmt = select(*select_items)

        if distinct:
            stmt = stmt.distinct()

        if where:
            for key, value in where.items():
                if key not in table.c:
                    raise ValueError(f"Column '{key}' not in table '{table_name}'")
                
                col = table.c[key]

                if isinstance(value, (list, tuple, set)):
                    stmt = stmt.where(col.in_(value))   # 🔥 IN
                else:
                    stmt = stmt.where(col == value) 

        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            rows = result.fetchall()
            keys = result.keys()

        # ---- DataFrame ----
        if as_dataframe:
            return pd.DataFrame(rows, columns=keys)

        # ---- Dict ----
        if as_dict:
            return [dict(zip(keys, row)) for row in rows]

        return rows
    
    def get_max_value(self,column_name, table_name,where=None):
       
        # 1. Base da query
        query_str = f"SELECT MAX({column_name}) FROM {table_name}"
        params = {}

        # 2. Construção dinâmica do WHERE (se existir)
        if where:
            conditions = []
            for key, value in where.items():
                # Usamos :key para bind parameters seguros
                conditions.append(f"{key} = :{key}")
                params[key] = value
            query_str += " WHERE " + " AND ".join(conditions)

        query = text(query_str)

        with self.engine.connect() as connection:
            result = connection.execute(query, params).fetchone()
            value = result[0] if result else None            
           
        
        return value