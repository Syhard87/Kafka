# Modèle illustratif : une semaine de sauvegardes

Exemple construit pour expliquer, pas une mesure. Hypothèses : 4 fichiers A, B, C, D ;
sauvegarde totale le dimanche ; un fichier modifié par jour (lundi B, mardi C, mercredi D) ;
panne le jeudi matin.

| Jour | Totale chaque jour | Incrémentale (depuis la veille) | Différentielle (depuis la totale) |
| :--- | :--- | :--- | :--- |
| Dimanche | A B C D | A B C D (totale) | A B C D (totale) |
| Lundi | A B C D | B | B |
| Mardi | A B C D | C | B C |
| Mercredi | A B C D | D | B C D |

Restauration le jeudi :

- totale chaque jour : 1 copie (celle de mercredi) ;
- incrémentale : 4 copies (dimanche, lundi, mardi, mercredi), dans l'ordre ;
- différentielle : 2 copies (dimanche et mercredi).

Volume copié du lundi au mercredi : totale 12 fichiers, incrémentale 3, différentielle 6.

Soit, en 3 jours (du lundi au mercredi) : 12 fichiers copiés en totale chaque jour, 3 fichiers en
incrémentale, 6 fichiers en différentielle.
