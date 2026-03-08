# Banking Database System

### Term Project – MIS / Database Systems

## 1. Project Overview
This project implements a simplified banking information system that demonstrates proper relational database design, transactional integrity, and analytical reporting.

The system separates responsibilities between:
- **PostgreSQL** for transactional (OLTP) operations
- **ClickHouse** for analytical (OLAP) workloads
- **Grafana** for visualization and dashboards

All components are deployed using Docker Compose on an Amazon EC2 instance.

## 2. System Architecture
### Components
- **PostgreSQL**  
  Primary system of record. Enforces constraints, foreign keys, and triggers. Stores customers, accounts, employees, managers, loans, and transactions.

- **ClickHouse**  
  Analytical database optimized for time-series aggregation. Receives mirrored transaction data from PostgreSQL. Used for high-volume analytics and dashboards.

- **Grafana**  
  Visualization layer. Connects to PostgreSQL (entity/state views) and ClickHouse (transaction analytics).

- **Data Generator**  
  Python service that continuously generates realistic banking transactions, writes to PostgreSQL, and mirrors to ClickHouse.

## 3. Database Design
### Core Tables
- branch
- account_type
- loan_type
- card_type
- customer
- customer_phone
- customer_email
- employee
- manager
- account
- card
- loan
- credit_score
- bank_transaction

### Key Design Decisions
- Managers are modeled using a separate `manager` table to clearly enforce **exactly one manager per branch**.
- Employees represent all staff roles.
- Transactions update account balances using a PostgreSQL trigger.
- ClickHouse stores only transaction data for analytics.

## 4. Business Rules and Enforcement
### One Manager per Branch
- Enforced using a **UNIQUE(branch_id)** constraint in the manager table.
- Each manager references **one employee** and **one branch**.

### Account Balance Integrity
- All balance changes occur through `bank_transaction`.
- A PostgreSQL trigger:
  - Validates transaction type
  - Prevents overdrafts
  - Updates balances atomically

### Referential Integrity
- Foreign keys enforce valid relationships between:
  - Customers and accounts
  - Accounts and transactions
  - Employees, managers, and branches
  - Loans and customers/accounts

## 5. Technology Stack
| Component             | Technology            |
| --------------------- | --------------------- |
| Database (OLTP)       | PostgreSQL 16         |
| Database (OLAP)       | ClickHouse            |
| Visualization         | Grafana               |
| Containerization      | Docker & Docker Compose |
| Hosting               | Amazon EC2            |
| Language              | Python (data generator) |

## 6. How to Run the Project
### Prerequisites
- Docker
- Docker Compose
- AWS EC2 (or any Linux server)

### Start All Services
```sh
docker compose up -d --build
```

### Check Status
```sh
docker compose ps
```

## 7. Accessing Services
### Grafana
- URL: `http://<EC2_PUBLIC_IP>:3000`
- Username: `admin`
- Password: `admin_pass_change_me`

### pgAdmin (PostgreSQL Web UI)
- URL: `http://<EC2_PUBLIC_IP>:5050`
- Email: `admin@bankingproject.com`
- Password: `admin123`

**PostgreSQL connection details inside pgAdmin:**
- Host: `postgres`
- Port: `5432`
- Database: `bankingdb`
- Username: `banking`
- Password: `banking_pass`

## 8. Example SQL Queries
**View a table**
```sql
SELECT * FROM public.customer LIMIT 5;
```

**View managers**
```sql
SELECT
  m.manager_id,
  e.first_name,
  e.last_name,
  b.branch_name
FROM public.manager m
JOIN public.employee e ON m.employee_id = e.employee_id
JOIN public.branch b ON m.branch_id = b.branch_id;
```

**View recent transactions**
```sql
SELECT *
FROM public.bank_transaction
ORDER BY transaction_date DESC
LIMIT 5;
```

## 9. Grafana Dashboards
The project includes dashboards that display:
- Table data from PostgreSQL (all entities)
- Transaction activity over time
- Transaction counts by type
- Recent transactions
- Aggregate metrics

Dashboards can be imported using the provided JSON files.

## 10. ER Diagram Explanation
- Employee represents all staff.
- Manager is a specialization that references Employee.
- Each Branch has exactly one Manager.
- This design simplifies understanding and grading while maintaining normalization.

## 11. Why PostgreSQL + ClickHouse?
- PostgreSQL ensures ACID compliance and transactional correctness.
- ClickHouse enables fast analytical queries without impacting OLTP performance.
- This separation reflects real-world enterprise database architecture.

## 12. Conclusion
This project demonstrates:
- Proper relational schema design.
- Enforcement of business rules at the database level.
- Clear separation of transactional and analytical workloads.
- Practical use of modern database and visualization tools.

It is designed to be robust, scalable, and easy to explain in an academic setting.
