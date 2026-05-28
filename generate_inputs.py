import argparse
import pandas as pd
import os

def main():
    parser = argparse.ArgumentParser(description="Generar datasets divididos con datos relacionados de un segundo archivo.")
    parser.add_argument("N", type=int, help="Número total de entradas a tomar")
    parser.add_argument("M", type=int, help="Número de datasets")
    parser.add_argument("--source", type=str, default="datasets/LI-Small_Trans.csv")
    # Nuevo argumento para el archivo relacionado
    parser.add_argument("--related", type=str, default="datasets/LI-Small_accounts.csv", help="datasets/LI-Small_accounts.csv")
    parser.add_argument("--join-on", type=str, default="Account", help="Account")
    parser.add_argument("--output-dir", type=str, default=".")
    args = parser.parse_args()

    # 1. Leer ambos archivos
    df_main = pd.read_csv(args.source)
    df_related = pd.read_csv(args.related)

    # 2. Muestreo del principal
    n_sample = min(args.N, len(df_main))
    sample = df_main.sample(n=n_sample)

    os.makedirs(args.output_dir, exist_ok=True)

    # 3. Lógica de división
    base_size = n_sample // args.M
    remainder = n_sample % args.M
    start = 0

    for i in range(args.M):
        size = base_size + (1 if i < remainder else 0)
        chunk_main = sample.iloc[start : start + size]
        start += size

        # 4. Filtrar el segundo archivo basándose en los IDs del chunk actual
        # Usamos .isin() para extraer solo las filas relacionadas
        ids_in_chunk = chunk_main[args.join_on]
        chunk_related = df_related[df_related[args.join_on].isin(ids_in_chunk)]

        # 5. Guardar ambos
        chunk_main.to_csv(os.path.join(args.output_dir, f"main_{i}.csv"), index=False)
        chunk_related.to_csv(os.path.join(args.output_dir, f"related_{i}.csv"), index=False)

    print(f"Generados {args.M} pares de archivos en '{args.output_dir}'.")

if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
