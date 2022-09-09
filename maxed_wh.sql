use role accountadmin;

-- create roles for wh_admin and wh_user
create role wh_admin;
grant create warehouse on account to role wh_admin;
grant create database on account to role wh_admin;
grant role wh_admin to user admin;

create role wh_user;
grant role wh_user to user admin;


-- create warehouse
use role wh_admin;
create warehouse maxedWH
    warehouse_size=xsmall
    auto_suspend=300
    auto_resume=true;

-- usage privilege allows wh_user to use the warehouse, but not modify the size
grant usage on warehouse maxedWH to role wh_user;


-- create home for the stp
create database procs;
create schema procs;
grant usage on database procs to role wh_user;
grant usage on schema procs to role wh_user;
use schema procs.procs;

-- create stored procedure with owner's rights (wh_admin's rights, ie modify privilege)
-- this STP allows wh_user to scale the warehouse between XS and Medium
create or replace procedure change_maxedWH(P_WH_NM varchar, P_WH_SIZE varchar)
    returns string
    language javascript
    execute as owner
    AS
$$
  var result = "";
  var sqlCmd = "";
  var sqlStmt = "";
  var rs = "";
  var curSize = "";
  var whSizesAllowed = ["X-SMALL", "XSMALL", "SMALL", "MEDIUM"];

  try {
    // first validate the warehouse exists and get the current size
    sqlCmd = "SHOW WAREHOUSES LIKE '" + P_WH_NM + "'";
    sqlStmt = snowflake.createStatement( {sqlText: sqlCmd} );
    rs = sqlStmt.execute();

    if (sqlStmt.getRowCount() == 0) {
      throw new Error('No Warehouse Found by that name');
    } else {
      rs.next();
      curSize = rs.getColumnValue('size').toUpperCase();
    }

    // next validate the new size is in the acceptable range
    if (whSizesAllowed.indexOf(P_WH_SIZE.toUpperCase()) == -1) {
      throw new Error('Not a valid Warehouse size');
    };

    // set Warehouse size
    sqlCmd = "ALTER WAREHOUSE " + P_WH_NM + " SET WAREHOUSE_SIZE = :1";
    sqlStmt = snowflake.createStatement( {sqlText: sqlCmd, binds: [P_WH_SIZE]} );
    sqlStmt.execute();

    result = "Resized Warehouse " + P_WH_NM + " from: " + curSize + " to: " + P_WH_SIZE.toUpperCase();
  }
  catch (err) {
    if (err.code === undefined) {
      result = err.message
    } else {
      result =  "Failed: Code: " + err.code + " | State: " + err.state;
      result += "\n  Message: " + err.message;
      result += "\nStack Trace:\n" + err.stackTraceTxt;
      result += "\nParam:\n" + P_WH_NM + ", " + P_WH_SIZE;
    }
  }
  return result;
$$;

-- allow wh_user to call the STP
grant usage on procedure change_maxedWH(varchar, varchar) to role wh_user;

-- switch to wh_user role
use role wh_user;

-- try to modify the warehouse traditional way; not allowed
alter warehouse maxedwh
    set warehouse_size=medium;

-- modify warehouse size using the STP
call change_maxedWH('maxedWH', 'medium');
-- Resized Warehouse maxedWH from: SMALL to: MEDIUM

call change_maxedWH('maxedWH', 'large');
-- Not a valid Warehouse size

-- clean up
use role wh_admin;
drop warehouse if exists maxedwh;
drop database if exists procs;
use role accountadmin;
drop role if exists wh_admin;
drop role if exists wh_user;
