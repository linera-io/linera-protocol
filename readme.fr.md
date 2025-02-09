# <img src="https://github.com/linera-io/linera-protocol/assets/1105398/fe08c941-93af-4114-bb83-bcc0eaec95f9" width="250" height="90" />

[![Licence](https://img.shields.io/badge/license-Apache-green.svg)](LICENSE)
[![Statut de compilation pour Rust](https://github.com/linera-io/linera-protocol/actions/workflows/rust.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/rust.yml)
[![Statut de compilation pour la documentation](https://github.com/linera-io/linera-protocol/actions/workflows/documentation.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/documentation.yml)
[![Statut de compilation pour DynamoDB](https://github.com/linera-io/linera-protocol/actions/workflows/dynamodb.yml/badge.svg)](https://github.com/linera-io/linera-protocol/actions/workflows/dynamodb.yml)

[Linera](https://linera.io) est une infrastructure blockchain décentralisée conçue pour des applications Web3 hautement évolutives et à faible latence.

Visitez notre [page développeur](https://linera.dev) et lisez notre [livre blanc](https://linera.io/whitepaper) pour en savoir plus sur le protocole Linera.

## Structure du dépôt

Les principaux crates et répertoires de ce dépôt peuvent être résumés comme suit (listés du plus bas au plus haut niveau dans le graphe de dépendance) :

* [`linera-base`](https://linera-io.github.io/linera-protocol/linera_base/index.html) Définitions de base, y compris la cryptographie.
* [`linera-version`](https://linera-io.github.io/linera-protocol/linera_version/index.html) Bibliothèque pour gérer les informations de version dans les binaires et services.
* [`linera-views`](https://linera-io.github.io/linera-protocol/linera_views/index.html) Bibliothèque de structures de données complexes basées sur un magasin clé-valeur.
* [`linera-execution`](https://linera-io.github.io/linera-protocol/linera_execution/index.html) Données persistantes et logique d'exécution des applications Linera.
* [`linera-chain`](https://linera-io.github.io/linera-protocol/linera_chain/index.html) Gestion des chaînes de blocs, certificats et messagerie inter-chaînes.
* [`linera-storage`](https://linera-io.github.io/linera-protocol/linera_storage/index.html) Abstractions de stockage pour le protocole.
* [`linera-core`](https://linera-io.github.io/linera-protocol/linera_core/index.html) Protocole central de Linera, y compris la logique client-serveur et la synchronisation des nœuds.
* [`linera-rpc`](https://linera-io.github.io/linera-protocol/linera_rpc/index.html) Définition des types de données pour les messages RPC.
* [`linera-client`](https://linera-io.github.io/linera-protocol/linera_client/index.html) Bibliothèque pour écrire des clients Linera.
* [`linera-service`](https://linera-io.github.io/linera-protocol/linera_service/index.html) Exécutable pour clients, proxy et serveurs.
* [`linera-sdk`](https://linera-io.github.io/linera-protocol/linera_sdk/index.html) Bibliothèque pour développer des applications Linera en Rust.
* [`examples`](./examples) Exemples d'applications Linera en Rust.

## Démarrage rapide avec l'interface CLI de Linera

Les commandes suivantes configurent un réseau de test local et effectuent des transferts entre des microchaînes détenues par un seul portefeuille.

```bash
# Assurez-vous de compiler les binaires Linera et de les ajouter au $PATH.
# cargo build -p linera-storage-service -p linera-service --bins --features storage-service
export PATH="$PWD/target/debug:$PATH"

# Importer la fonction d'aide optionnelle `linera_spawn_and_read_wallet_variables`.
source /dev/stdin <<<"$(linera net helper 2>/dev/null)"

# Démarrer un réseau de test local.
linera_spawn_and_read_wallet_variables \
linera net up

# Afficher la liste des validateurs.
linera query-validators

# Vérifier le solde des chaînes.
CHAIN1="aee928d4bf3880353b4a3cd9b6f88e6cc6e5ed050860abae439e7782e9b2dfe8"
CHAIN2="a3edc33d8e951a1139333be8a4b56646b5598a8f51216e86592d881808972b07"
linera query-balance "$CHAIN1"
linera query-balance "$CHAIN2"

# Transférer 10 unités puis 5 unités en retour.
linera transfer 10 --from "$CHAIN1" --to "$CHAIN2"
linera transfer 5 --from "$CHAIN2" --to "$CHAIN1"

# Vérifier les soldes à nouveau.
linera query-balance "$CHAIN1"
linera query-balance "$CHAIN2"
```

Des exemples plus complexes sont disponibles dans notre [manuel développeur](https://linera.dev) ainsi que dans les [applications d'exemple](./examples) de ce dépôt.

