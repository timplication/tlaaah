#![allow(dead_code, unused_variables, unused_imports)]

use duckdb::arrow::record_batch::RecordBatch;
use duckdb::arrow::util::pretty::print_batches;
use duckdb::{params, Connection, Result};

#[derive(Debug)]
struct State {
    state_id: u64,
    is_initial: bool,
}

#[derive(Debug)]
struct Transition {
    from_state: u64,
    to_state: u64,
}

#[derive(Debug)]
struct Predicate {
    fact_id: u64,
    state_id: u64,
    name: String,
    attr1: Option<String>,
    attr2: Option<String>,
    attr3: Option<String>,
}

#[derive(Debug)]
struct PredicateQuery {
    name: String,
    attr1: Option<String>,
    attr2: Option<String>,
    attr3: Option<String>,
}

fn create_tables(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r" CREATE TABLE state (
               state_id UBIGINT PRIMARY KEY,
               is_initial BOOLEAN,
           );

           CREATE TABLE transition (
               from_state UBIGINT,
               to_state UBIGINT,
               PRIMARY KEY (from_state, to_state)
           );

           CREATE TABLE predicate (
               fact_id UBIGINT PRIMARY KEY,
               state_id UBIGINT,
               name VARCHAR,
               attr1 VARCHAR,
               attr2 VARCHAR,
               attr3 VARCHAR,
            ); ",
    )?;

    Ok(())
}

fn insert_predicate(conn: &Connection, pred: Predicate) -> Result<()> {
    conn.execute(
        r"INSERT INTO predicate (fact_id, state_id, name, attr1, attr2, attr3)
          VALUES (?1, ?2, ?3, ?4, ?5, ?6);
        ",
        params![
            pred.fact_id,
            pred.state_id,
            pred.name,
            pred.attr1,
            pred.attr2,
            pred.attr3,
        ],
    )?;

    Ok(())
}

fn insert_state(conn: &Connection, state: State) -> Result<()> {
    conn.execute(
        r"INSERT INTO state (state_id, is_initial)
              VALUES (?1, ?2);",
        params![state.state_id, state.is_initial,],
    )?;

    Ok(())
}

fn insert_transition(conn: &Connection, trans: Transition) -> Result<()> {
    conn.execute(
        r"INSERT INTO transition (from_state, to_state)
              VALUES (?1, ?2);",
        params![trans.from_state, trans.to_state],
    )?;

    Ok(())
}

fn state_formula_predicate(pred: PredicateQuery, state_id: u64) -> String {
    format!(
        r"
            SELECT EXISTS(
            SELECT *
            FROM state s JOIN predicate p ON s.state_id = p.state_id
            WHERE p.name = '{}' AND p.attr1 {} AND p.attr2 {} AND p.attr3 {}
            AND s.state_id = {})
        ",
        pred.name,
        pred.attr1
            .map(|v| format!("= '{}'", v))
            .unwrap_or(String::from("IS NULL")),
        pred.attr2
            .map(|v| format!("= '{}'", v))
            .unwrap_or(String::from("IS NULL")),
        pred.attr3
            .map(|v| format!("= '{}'", v))
            .unwrap_or(String::from("IS NULL")),
        state_id
    )
}

fn state_formula_not(subformula: String) -> String {
    format!("SELECT NOT EXISTS ({})", subformula)
}

fn state_formula_and(subformula_1: String, subformula_2: String) -> String {
    format!(
        "SELECT EXISTS ({}) AND EXISTS({})",
        subformula_1, subformula_2
    )
}

fn main() -> Result<()> {
    // TLA+ spec we are transcribing:
    //
    // VARIABLE b
    //
    // INIT == b=0 \/ b=1
    //
    // Next == \/ b=0 /\ b’=1
    //         \/ b=1 /\ b’=0
    //
    let conn = Connection::open_in_memory()?;
    create_tables(&conn)?;

    insert_state(
        &conn,
        State {
            state_id: 0,
            is_initial: true,
        },
    )?;
    insert_state(
        &conn,
        State {
            state_id: 1,
            is_initial: true,
        },
    )?;
    insert_predicate(
        &conn,
        Predicate {
            fact_id: 0,
            state_id: 0,
            name: String::from("b"),
            attr1: Some(String::from("0")),
            attr2: None,
            attr3: None,
        },
    )?;
    insert_predicate(
        &conn,
        Predicate {
            fact_id: 1,
            state_id: 1,
            name: String::from("b"),
            attr1: Some(String::from("1")),
            attr2: None,
            attr3: None,
        },
    )?;
    insert_transition(
        &conn,
        Transition {
            from_state: 0,
            to_state: 1,
        },
    )?;
    insert_transition(
        &conn,
        Transition {
            from_state: 1,
            to_state: 0,
        },
    )?;

    let pred_query = state_formula_predicate(
        PredicateQuery {
            name: String::from("b"),
            attr1: Some(String::from("1")),
            attr2: None,
            attr3: None,
        },
        1,
    );

    // if this returns true, the predicate is true.
    let mut stmt = conn.prepare(&pred_query)?;
    print_batches(&stmt.query_arrow([])?.collect::<Vec<_>>()).unwrap();

    // if this returns true, NOT(predicate) is true.
    let formula2 = state_formula_not(pred_query.clone());
    let mut stmt2 = conn.prepare(&formula2)?;
    print_batches(&stmt2.query_arrow([])?.collect::<Vec<_>>()).unwrap();

    // if this returns true, (predicate) AND (predicate) is true.
    let formula3 = state_formula_and(pred_query.clone(), pred_query.clone());
    let mut stmt3 = conn.prepare(&formula3)?;
    print_batches(&stmt3.query_arrow([])?.collect::<Vec<_>>()).unwrap();

    Ok(())
}
