input_events = [

    # --- Day 1 ---
    "2024-01-01 user=1 amount=100.0 currency=USD",
    "2024-01-01 user=2 amount=50 currency=USD",
    "2024-01-01 user=1 amount=100.0 currency=USD",  # duplicate
    "2024-01-01 user=3 amount=25 currency=USD",

    # --- Malformed ---
    "2024-01-01 user=abc amount=30 currency=USD",   # bad user
    "2024-01-01 user=4 amount=-20 currency=USD",    # negative
    "2024-01-01 user=5 amount=10 currency=EUR",     # wrong currency
    "not-a-date user=6 amount=15 currency=USD",     # bad date

    # --- Day 2 ---
    "2024-01-02 user=1 amount=75 currency=USD",
    "2024-01-02 user=2 amount=125 currency=USD",
    "2024-01-02 user=3 amount=60 currency=USD",

    # --- Duplicate block (replay simulation) ---
    "2024-01-02 user=1 amount=75 currency=USD",
    "2024-01-02 user=2 amount=125 currency=USD",

    # --- More valid ---
    "2024-01-02 user=4 amount=200 currency=USD",
    "2024-01-03 user=1 amount=300 currency=USD",

    # --- More malformed ---
    "",
    None,
    "2024-01-03 user=2 amount=oops currency=USD",

    # --- Day 3 ---
    "2024-01-03 user=2 amount=150 currency=USD",
    "2024-01-03 user=3 amount=100 currency=USD",

    # --- Intentional late duplicate ---
    "2024-01-01 user=2 amount=50 currency=USD",

    # --- More load ---
    "2024-01-04 user=1 amount=500 currency=USD",
    "2024-01-04 user=2 amount=400 currency=USD",
    "2024-01-04 user=3 amount=250 currency=USD",
    "2024-01-04 user=4 amount=100 currency=USD",

    # --- Replay whole day 4 ---
    "2024-01-04 user=1 amount=500 currency=USD",
    "2024-01-04 user=2 amount=400 currency=USD",

    # --- Day 5 ---
    "2024-01-05 user=1 amount=1000 currency=USD",
    "2024-01-05 user=5 amount=50 currency=USD",
]