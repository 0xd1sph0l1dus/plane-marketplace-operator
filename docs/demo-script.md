# Script de démo — répétition entretien

> Script de répétition personnel. Durée cible : **8-10 minutes**.
> Avant de commencer : vérifier contexte k3d, variables AWS edge exportées,
> opérateur lancé (`make run` dans un terminal dédié, visible à l'écran).

## 0. Setup (avant l'entretien, 5 min)

```bash
kubectl config use-context k3d-a380-01        # JAMAIS le k3s perso
source /tmp/edge-creds.env && unset AWS_PROFILE
helm ls -n marketplace                          # demo-app + streaming-film présents
kubectl get apprelease streaming-film           # Ready=True, 2.4.0 installé
```

Vérifier aussi : bucket S3 restauré (`aws s3 ls s3://marketplace-artifacts-...`),
`make run` actif dans un terminal visible.

## 1. Pitch (30 s, pendant que les écrans se préparent)

"Je vais vous montrer un opérateur Kubernetes écrit en Go qui gère le cycle de vie
d'applications d'une marketplace embarquée sur une flotte d'avions. Le principe :
l'avion est un cluster K3s avec une connectivité intermittente ; le cloud AWS
(S3 + SQS) exprime l'état désiré ; l'opérateur converge, vérifie l'intégrité de
chaque artefact, et remonte l'état réel. On va dérouler 4 scénarios : installation,
mise à jour, artefact corrompu, rollback."

## 2. Scénario T1 — Installation (1 min 30)

**Raconter** : "Voici le CR `AppRelease` — un bon de commande. L'avion tourne déjà
en 2.4.0, le status le confirme. Regardons la boucle en action."

```bash
kubectl delete apprelease streaming-film
kubectl apply -f config/samples/marketplace_v1alpha1_apprelease.yaml
kubectl get pods -n marketplace -w     # le pod renaît sous les yeux
```

Point à dire : "la suppression + réapplication = le CR repart de zéro, l'opérateur
détecte le drift, télécharge depuis S3, vérifie le sha256, installe via le SDK
Helm, et écrit le status. Tout ça sans redémarrer quoi que ce soit."

## 3. Scénario T2 — Mise à jour (1 min)

```bash
kubectl edit apprelease streaming-film   # version: "2.4.0" → "2.5.0"
helm ls -n marketplace                   # REVISION +1
```

Point à dire : "une mise à jour = un changement de la spec. L'upgrade est atomique :
si Helm échoue, il remet automatiquement la version précédente."

## 4. Scénario T3 — Artefact corrompu (2 min, LE moment)

```bash
echo "corrupted in transit" | aws s3 cp - s3://marketplace-artifacts-<compte>/demo-app-0.1.0.tgz
kubectl edit apprelease streaming-film   # version → "2.6.0"
kubectl get apprelease streaming-film -o jsonpath='{.status.conditions} {"\n"}'
# → Ready: False, reason: DigestMismatch
kubectl get pods -n marketplace          # l'app 2.5.0 tourne TOUJOURS
```

Points à dire : "la liaison a corrompu le transfert. L'opérateur télécharge, recalcule
le sha256, détecte la différence, jette le fichier et refuse d'installer — l'avion
n'est jamais dégradé. C'est l'exigence d'intégrité qui rend la distribution edge
sûre." Puis : "le cloud répare" :

```bash
aws s3 cp /tmp/demo-app-0.1.0.tgz s3://marketplace-artifacts-<compte>/demo-app-0.1.0.tgz
kubectl edit apprelease streaming-film   # n'importe quel champ pour re-trigger
# → l'upgrade 2.6.0 réussit
```

## 5. Scénario T4 — Rollback (1 min)

```bash
kubectl edit apprelease streaming-film   # version → "2.4.0"
helm ls -n marketplace                   # nouvelle révision, retour en arrière
```

Point à dire : "le rollback est trivial parce que tout est déclaratif et versionné :
on réécrit la version désirée, et le bucket S3 versionné garde l'historique des
artefacts."

## 6. Scénario T5 — Déconnexion (si l'agent est prêt, 2 min)

```bash
# envoi d'un message dans la queue desired pendant que l'agent est coupé
kill <pid edge-agent>
aws sqs send-message --queue-url <desired-url> --message-group-id a380-01 \
  --message-body '{"aircraft":"a380-01","appName":"streaming-film","version":"2.7.0",...}'
# relance de l'agent → le CR apparaît, l'install part, le status remonte
```

Points à dire : "SQS FIFO + déduplication : le message rejoué par la liaison
instable ne s'applique qu'une fois. Le backoff exponentiel évite de marteler le
réseau satellite. Au retour de la liaison, resync complet."

## 7. Fermeture (30 s)

"En résumé : un opérateur standard Kubebuilder côté edge, un control plane AWS
côté cloud, et une sync asynchrone tolérante aux coupures. Le code est sur GitHub,
les choix sont documentés dans le README."

## Plan B (si ça plante en live)

- Crash de `make run` → relancer : `source /tmp/edge-creds.env && make run`
  (le CR conserve son état, la boucle reprend toute seule — argument de résilience).
- Mauvais contexte kubectl → `kubectl config use-context k3d-a380-01`.
- AWS en erreur → montrer les schémas du README et raconter les scénarios
  (tout est déjà validé et documenté).
- Question sur la sync cloud→edge → renvoyer vers l'architecture cible
  (agent + SQS FIFO) et la roadmap du README.

## Anti-sèche questions probables

- *Pourquoi un opérateur et pas un cronjob/Argo CD ?* → état déclaratif,
  auto-réparation, CRD standardisée ; Argo CD gérerait le drift *dans* le cluster,
  pas la sync flotte avec connectivité intermittente (complémentaire).
- *Comment gérez-vous les conflits de spec ?* → le cloud est la source de vérité ;
  le status est écrit uniquement par l'edge ; dernier message gagne via l'ordre FIFO.
- *Que se passe-t-il si deux avions partagent la queue ?* → message-group-id par
  avion = files logiques indépendantes dans la même queue.
- *Sécurité des artefacts ?* → sha256 dans la spec aujourd'hui ; signature (cosign)
  en roadmap ; IAM least-privilege ; jamais de secrets dans le repo.
