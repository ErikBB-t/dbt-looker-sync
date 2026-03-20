view: users {
  sql_table_name: `my_schema.users` ;;

  dimension: user_id {
    primary_key: yes
    type: number
    sql: ${TABLE}.user_id ;;
    description: "Unique identifier for a user."
  }

  dimension: name {
    type: string
    description: "User's full name."
    sql: ${TABLE}.name ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: created_at {
    type: string
    sql: ${TABLE}.created_at ;;
    description: "Timestamp when the user was created."
  }

  measure: count {
    type: count
  }
}