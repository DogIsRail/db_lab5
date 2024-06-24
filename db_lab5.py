from neo4j import GraphDatabase, basic_auth
from pprint import pprint

driver = GraphDatabase.driver( 
  'neo4j+ssc://af8c5e98.databases.neo4j.io:7687', 
  auth=basic_auth('neo4j', 'VTYFrtUIyyiqN409-iKe_AcS43KDjSh1d4hyiyF2Q1w'))

delete_query = [
    "MATCH (i:Item) DETACH DELETE i;",
    "MATCH (c:Customer) DETACH DELETE c;",
    "MATCH (o:Order) DETACH DELETE o;"
]

create_query = """
CREATE 
(i1:Item {item_id: 1, item_name: "DJI Mavic Air 2", price: 799}),
(i2:Item {item_id: 2, item_name: "LG InstaView Door-in-Door", price: 2199}),
(i3:Item {item_id: 3, item_name: "Amazon Echo (4th Gen)", price: 99}),
(i4:Item {item_id: 4, item_name: "Nikon Z6 II", price: 1999}),
(i5:Item {item_id: 5, item_name: "Panasonic NN-SN966S", price: 129}),
(i6:Item {item_id: 6, item_name: "Kindle Paperwhite", price: 799}),

(c1:Customer {customer_id: 1, name: "Dmytro"}),
(c2:Customer {customer_id: 2, name: "Olena"}),
(c3:Customer {customer_id: 3, name: "Ivan"}),
(c4:Customer {customer_id: 4, name: "Yulia"}),
(c5:Customer {customer_id: 5, name: "Mykola"}),

(o1:Order {order_id: 1, date: "2024-04-15"}),
(o2:Order {order_id: 2, date: "2024-04-16"}),
(o3:Order {order_id: 3, date: "2024-04-17"}),
(o4:Order {order_id: 4, date: "2024-04-18"}),
(o5:Order {order_id: 5, date: "2024-04-19"})
"""

bought_query = """
MATCH (c:Customer {customer_id: $customer_id}), (o:Order {order_id: $order_id})
MERGE (c)-[:BOUGHT]->(o)
"""

contains_query = """
MATCH (o:Order {order_id: $order_id}), (i:Item {item_id: $item_id})
MERGE (o)-[:CONTAINS]->(i)
"""

view_query = """
MATCH (c:Customer {customer_id: $customer_id}), (i:Item {item_id: $item_id})
MERGE (c)-[:VIEW]->(i)
"""


def create_data():
    with driver.session() as session:
        for query in delete_query:
            session.run(query)
        session.run(create_query)


def add_contains_relationship(order_id, item_id):
    with driver.session() as session:
        session.run(contains_query, order_id=order_id, item_id=item_id)


def add_bought_relationship(customer_id, order_id):
    with driver.session() as session:
        session.run(bought_query, customer_id=customer_id, order_id=order_id)


def add_view_relationship(customer_id, item_id):
    with driver.session() as session:
        session.run(view_query, customer_id=customer_id, item_id=item_id)

create_data()

add_contains_relationship(1, 1)
add_contains_relationship(1, 2)
add_contains_relationship(2, 3)
add_contains_relationship(3, 5)
add_contains_relationship(3, 7)
add_contains_relationship(3, 2)
add_contains_relationship(4, 1)
add_contains_relationship(5, 2)
add_contains_relationship(5, 4)

add_bought_relationship(1, 1)
add_bought_relationship(2, 2)
add_bought_relationship(2, 1)
add_bought_relationship(3, 5)
add_bought_relationship(4, 3)
add_bought_relationship(4, 2)
add_bought_relationship(5, 4)

add_view_relationship(1, 2)
add_view_relationship(1, 3)
add_view_relationship(3, 5)
add_view_relationship(3, 1)
add_view_relationship(3, 2)
add_view_relationship(6, 6)


# 1
print("Request 1:")
def find_items_in_order(order_id):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (o:Order {order_id: $order_id})-[:CONTAINS]->(i:Item)
            RETURN i
            """,
            order_id=order_id
        )
        items = [record["i"]._properties for record in result]
        return items


order_id = 1
items_in_order = find_items_in_order(order_id)
pprint(items_in_order)
print()

# 2
print("Request 2:")
def get_order_total(order_id):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (o:Order {order_id: $order_id})-[:CONTAINS]->(i:Item)
            RETURN SUM(i.price) AS total_cost
            """,
            order_id=order_id
        )
        total_cost = result.single()["total_cost"]
        return total_cost


order_id = 1
total_cost = get_order_total(order_id)
print(f"Total cost of order {order_id}: {total_cost}")
print()

# 3
print("Request 3:")
def find_orders_by_customer(customer_id):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (c:Customer {customer_id: $customer_id})-[:BOUGHT]->(o:Order)
            RETURN o
            """,
            customer_id=customer_id
        )
        orders = [record["o"]._properties for record in result]
        return orders


customer_id = 4
orders = find_orders_by_customer(customer_id)
pprint(orders)
print()

# 4
print("Request 4:")
def find_items_by_customer(customer_id):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (c:Customer {customer_id: $customer_id})-[:BOUGHT]->(:Order)-[:CONTAINS]->(i:Item)
            RETURN i
            """,
            customer_id=customer_id
        )
        items = [record["i"]._properties for record in result]
        return items


customer_id = 4
items = find_items_by_customer(customer_id)
pprint(items)
print()

# 5
print("Request 5:")
def count_items_bought_by_customer(customer_id):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (c:Customer {customer_id: $customer_id})-[:BOUGHT]->(:Order)-[:CONTAINS]->(i:Item)
            RETURN COUNT(i) AS total_items_bought
            """,
            customer_id=customer_id
        )
        total_items_bought = result.single()["total_items_bought"]
        return total_items_bought


customer_id = 4
total_items_bought = count_items_bought_by_customer(customer_id)
print("Total items bought by customer:", total_items_bought)
print()

# 6
print("Request 6:")
def total_spent_by_customer(customer_id):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (c:Customer {customer_id: $customer_id})-[:BOUGHT]->(o:Order)-[:CONTAINS]->(i:Item)
            RETURN SUM(i.price) AS total_spent_by_customer
            """,
            customer_id=customer_id
        )
        total_spent_by_customer = result.single()["total_spent_by_customer"]
        return total_spent_by_customer


customer_id = 4
total_spent = total_spent_by_customer(customer_id)
print("Total spent by customer:", total_spent)
print()

# 7
print("Request 7:")
def count_purchases_per_item():
    with driver.session() as session:
        result = session.run(
            """
            MATCH (i:Item)<-[:CONTAINS]-(:Order)
            RETURN i, COUNT(*) AS purchases
            ORDER BY purchases DESC
            """
        )
        purchases_per_item = [(record["i"], record["purchases"]) for record in result]
        return purchases_per_item


purchases_per_item = count_purchases_per_item()
for item, purchases in purchases_per_item:
    print(f"Item {item['item_id']} was purchased {purchases} times")
print()

# 8
print("Request 8:")
def find_viewed_items(customer_id):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (c:Customer {customer_id: $customer_id})-[:VIEW]->(i:Item)
            RETURN i
            """,
            customer_id=customer_id
        )
        viewed_items = [record["i"]._properties for record in result]
        return viewed_items

customer_id = 1
viewed_items = find_viewed_items(customer_id)
pprint(viewed_items)
print()

# 9
print("Request 9:")
def find_related_items(item_id):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (i1:Item {item_id: $item_id})<-[:CONTAINS]-(o:Order)-[:CONTAINS]->(i2:Item)
            WHERE i1 <> i2
            RETURN DISTINCT i2
            """,
            item_id=item_id
        )
        items = [record["i2"]._properties for record in result]
        return items

item_id = 5
related_items = find_related_items(item_id)
pprint(related_items)
print()

# 10
print("Request 10:")
def find_customers_who_bought_item(item_id):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (c:Customer)-[:BOUGHT]->(:Order)-[:CONTAINS]->(i:Item)
            WHERE i.item_id = $item_id
            RETURN DISTINCT c
            """,
            item_id=item_id
        )
        customers = [record["c"]._properties for record in result]
        return customers

item_id = 1
customers = find_customers_who_bought_item(item_id)
pprint(customers)
print()

# 11
print("Request 11:")
def find_items_viewed_but_not_bought(customer_id):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (c:Customer {customer_id: $customer_id})-[:VIEW]->(i:Item)
            WHERE NOT EXISTS {
                (c)-[:BOUGHT]->(:Order)-[:CONTAINS]->(i)
            }
            RETURN i
            """,
            customer_id=customer_id
        )
        items = [record["i"]._properties for record in result]
        return items

customer_id = 1
items = find_items_viewed_but_not_bought(customer_id)
pprint(items)
print()
