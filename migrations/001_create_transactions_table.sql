-- Run this in the Supabase SQL Editor before importing transactions.
-- It creates the table and a unique constraint used for duplicate detection.

create table if not exists public.transactions (
    id uuid default gen_random_uuid() primary key,
    entrada_saida text,
    data date,
    movimentacao text,
    produto text,
    produto_ticker text,
    instituicao text,
    quantidade numeric,
    preco_unitario numeric,
    valor_operacao numeric,
    row_hash text not null unique,
    source_file text,
    imported_at timestamptz default now()
);

create index if not exists idx_transactions_data on public.transactions(data);
create index if not exists idx_transactions_produto_ticker on public.transactions(produto_ticker);
create index if not exists idx_transactions_row_hash on public.transactions(row_hash);
