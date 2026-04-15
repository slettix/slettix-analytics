"""
Registrer DuckDB-tilkobling mot Slettix Delta Lake i Superset.
Kjøres én gang av superset-init-tjenesten etter `superset init`.
"""
try:
    from superset.app import create_app

    flask_app = create_app()
    with flask_app.app_context():
        from superset.extensions import db
        from superset.models.core import Database

        existing = db.session.query(Database).filter_by(
            database_name="Slettix Delta Lake"
        ).first()

        if existing:
            print("DuckDB-tilkobling finnes allerede — hopper over.")
        else:
            conn = Database(
                database_name="Slettix Delta Lake",
                sqlalchemy_uri="duckdb:///:memory:",
                expose_in_sqllab=True,
                allow_run_async=False,
                allow_ctas=False,
                allow_cvas=False,
                allow_dml=False,
            )
            db.session.add(conn)
            db.session.commit()
            print("✓ DuckDB-tilkobling 'Slettix Delta Lake' registrert.")

except Exception as e:
    print(f"[ADVARSEL] Kunne ikke auto-registrere DuckDB: {e}")
    print("Legg til manuelt i Superset UI:")
    print("  Settings → Database Connections → + Database → DuckDB")
    print("  SQLAlchemy URI: duckdb:///:memory:")
