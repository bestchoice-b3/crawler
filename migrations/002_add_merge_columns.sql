-- Add columns needed for the merge UI (FIFO matching of sells and buys).

alter table public.transactions
    add column if not exists balance numeric,
    add column if not exists profit numeric,
    add column if not exists total numeric,
    add column if not exists merged_children jsonb default '[]'::jsonb;

-- Fill sensible defaults for existing rows.
update public.transactions
set balance = quantidade,
    profit = 0,
    total = 0,
    merged_children = '[]'::jsonb
where balance is null;
