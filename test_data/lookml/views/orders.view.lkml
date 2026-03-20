view: orders {
  sql_table_name: `my_schema.orders` ;;

  dimension: order_id {
    primary_key: yes
    type: number
    description: "The unique ID of the order."
    sql: ${TABLE}.order_id ;;
  }

  dimension: user_id {
    type: number
    description: "ID of the user who placed the order."
    sql: ${TABLE}.user_id ;;
  }

  dimension: amount {
    type: number
    description: "The order amount."
    sql: ${TABLE}.amount ;;
  }

  dimension: ordered_at {
    type: time
    description: "Timestamp when the order was placed."
    sql: ${TABLE}.ordered_at ;;
  }

  measure: count {
    type: count
  }
}
