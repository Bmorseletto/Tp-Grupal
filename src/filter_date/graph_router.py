import os
import csv
import fcntl

COMPONENTS_FILE = "/output/q4_graph_components.csv"


class GraphRouterCSV:
    # Arma nodos y componentes para agrupar los pares bancos-cuentas de un mismo flujo: A->B->C
    # Osea: matcheamos con banco-cuenta origen y banco-cuenta destino y asi tenemos flujo completo
    def __init__(self, num_nodes):
        self.num_nodes = num_nodes
        os.makedirs(os.path.dirname(COMPONENTS_FILE), exist_ok=True)
        if not os.path.exists(COMPONENTS_FILE):
            with open(COMPONENTS_FILE, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["rep", "comp_id"])
                writer.writeheader()

    def _load_components(self):
        components = {}
        with open(COMPONENTS_FILE, "r", newline="") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            reader = csv.DictReader(f)
            for row in reader:
                components[row["rep"]] = int(row["comp_id"])
            fcntl.flock(f, fcntl.LOCK_UN)
        return components

    def _rewrite_components(self, components):
        # Reescribe todo el CSV con el dict actualizado
        with open(COMPONENTS_FILE, "w", newline="") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            writer = csv.DictWriter(f, fieldnames=["rep", "comp_id"])
            writer.writeheader()
            for rep, comp_id in components.items():
                writer.writerow({"rep": rep, "comp_id": comp_id})
            fcntl.flock(f, fcntl.LOCK_UN)

    def get_node(self, to_bank, to_account, from_bank, from_account):
        rep_to = f"{to_bank}:{to_account}"
        rep_fr = f"{from_bank}:{from_account}"
        components = self._load_components()

        if rep_to not in components and rep_fr not in components:
            # nuevo componente
            comp_id = len(components)
            components[rep_to] = comp_id
            components[rep_fr] = comp_id
            self._rewrite_components(components)
        elif rep_to in components and rep_fr not in components:
            comp_id = components[rep_to]
            components[rep_fr] = comp_id
            self._rewrite_components(components)
        elif rep_fr in components and rep_to not in components:
            comp_id = components[rep_fr]
            components[rep_to] = comp_id
            self._rewrite_components(components)
        else:
            # ambos existen: si tienen comp_id distinto, unificar
            comp_id_to = components[rep_to]
            comp_id_fr = components[rep_fr]
            if comp_id_to != comp_id_fr:
                # normalizar: todos los reps con comp_id_fr pasan a comp_id_to
                for rep, cid in components.items():
                    if cid == comp_id_fr:
                        components[rep] = comp_id_to
                comp_id = comp_id_to
                self._rewrite_components(components)
            else:
                comp_id = comp_id_to

        routing_key = "Q4Graph" + str(comp_id % self.num_nodes)
        return routing_key
