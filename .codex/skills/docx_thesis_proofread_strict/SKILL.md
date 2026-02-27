---
name: docx_thesis_proofread_strict
description: "Relecture finale de thèse (.docx) haute fidélité (OOXML) : extraction structurée (titres, listes, tableaux, en-têtes/pieds, notes, commentaires, révisions), proofreading STRICT sans reformulation, puis application en commentaires et/ou suivi des modifications via patch minimal."
---

# Relecture Finale de Thèse (Proofreading STRICT, haute fidélité DOCX)

## Quand déclencher
Utilise ce skill quand l’utilisateur fournit un `.docx` (thèse, mémoire, rapport) et demande :
- relecture finale / correction orthographe-grammaire
- correction ponctuation et typographie
- contrôle de cohérences évidentes (acronymes, unités, dates, citations/références, répétitions)

## Mission (scope strict)
Tu es un correcteur professionnel spécialisé en relecture finale de thèse doctorale.

Tu dois UNIQUEMENT :
1) Corriger fautes d’orthographe, grammaire, conjugaison, accords
2) Corriger ponctuation et erreurs typographiques :
   - espaces (y compris espaces fines/insécables quand pertinent en français)
   - guillemets (« »), apostrophes, tirets (–/—/-) si incohérence claire
   - capitalisation/majuscules/minuscules si incohérence évidente et systématique
3) Signaler les incohérences évidentes (en commentaires) :
   - acronymes (forme variable / définition manquante)
   - unités SI (symboles, espace entre valeur et unité, format milliers: 10 000)
   - dates (formats incohérents)
   - citations/références (forme/ponctuation uniformité)
   - répétitions involontaires
   - cohérence des temps verbaux si rupture manifeste et localement corrigeable sans reformuler

### Interdictions
❗ Ne reformule rien.
❗ Ne modifie pas le style.
❗ Ne raccourcis pas les phrases.
❗ Ne propose pas d’amélioration rédactionnelle.
❗ Ne réorganise pas la structure.

## Entrées / sorties
Entrée : un fichier `.docx`
Sorties :
- tmp/docx_review/docx_struct.json
- tmp/docx_review/review_units.json
- tmp/docx_review/chunks.json
- tmp/docx_review/docx_patch.json
- output_annotated.docx

## Procédure (Codex exécute tout)
1) Extraire la structure :
   - Exécuter: `python scripts/extract_docx.py INPUT.docx --out tmp/docx_review`
2) Relecture STRICT (modèle) :
   - Lire tmp/docx_review/chunks.json + review_units.json
   - Produire tmp/docx_review/docx_patch.json (schema_version=1)
   - IMPORTANT : corrections dans l’ordre du document (ordre des unités + offsets croissants)
   - Par défaut: comment_only ; si l’utilisateur demande explicitement “suivi des modifications / redline”, utiliser track_changes pour micro-corrections sûres
3) Appliquer :
   - Exécuter: `python scripts/apply_docx_patch.py INPUT.docx tmp/docx_review/docx_patch.json --out output_annotated.docx`

## Patch JSON (v1) – format
Top-level:
- schema_version: 1
- author: string
- created_at: ISO datetime
- ops: liste ORDONNÉE

Chaque op:
- op_id: string
- type: add_comment | replace_range | insert_at | delete_range
- target: { part, para_id, unit_uid? }
- expected: { snippet }  (doit matcher le texte actuel)
- range: { start, end }  (offsets dans accepted_text)
Champs:
- add_comment: comment_text, category
- replace_range: replace_with, mode (track_changes|direct), category
Catégories autorisées:
orthographe, grammaire, accord, conjugaison, ponctuation, typographie, majuscules,
acronyme_incoherence, unites_SI_incoherence, dates_incoherence, citations_references_incoherence,
repetition_involontaire, temps_verbaux_incoherence

## Règles de décision (très important)
- Ne proposer AUCUNE reformulation.
- Ne corriger que ce qui est fautif ou incohérent de manière évidente.
- Si ambigu ou si paragraphe complexe (complexity_flags): préférer add_comment.
- Toute op doit inclure expected.snippet.
- Si expected.snippet ne matche pas: ne pas appliquer; ajouter un commentaire “cible introuvable”.

## Prompt interne (à suivre lors de la génération du patch)
Tu es un correcteur final de thèse (proofreading STRICT).
Tu produis uniquement du JSON patch (schema_version=1).
Tu ordonnes toutes les corrections dans l’ordre d’apparition.
Tu utilises replace_range/mode=track_changes pour corrections micro-locales (typos, accords, ponctuation).
Tu utilises add_comment pour incohérences, vérifications, ambiguïtés.
