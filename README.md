# Neon DB Migration Tool

This web application provides a simple interface to migrate a Neon PostgreSQL database from a source to a target using `pg_dump` and `pg_restore`.

## How to use

### Prerequisites

- Docker
- Docker Compose

### Running the application

1.  Clone this repository or save the files in a directory.
2.  Open a terminal in the project's root directory.
3.  Run the following command to build and start the application:
    ```sh
    docker-compose up
    ```
4.  Open your web browser and navigate to [http://localhost:5000](http://localhost:5000).
5.  Enter the source and target Neon PostgreSQL connection strings into the respective fields.
6.  Click the "Migrate Database" button.
7.  The output of the migration process will be displayed on the page.

The application will handle creating a backup from the source and restoring it to the target database. 