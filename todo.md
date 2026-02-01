- [x] permettre un retour de résultats
- [x] voir comment le status peut être updater
    - quand le worker prend le message il le pop alors il est pas possible d'updater le status, 
- [x] refaire le système de logging
- [x] revoir la config
- [ ] ajouter un système de hook redis (BLPOP / XREAD BLOCK), pour éviter de faire contament des appel au redis
- [ ] créé un schéma de protocol de message, pour pouvoir avoir des worker dans différent langages
- [ ] déplacer le core.exception dans le utils
- [ ] vérifier la cohérence du code et le nétoyer
- [ ] faire la doc, readme
    - faire les cycles de vie d'un worker, orchestrateur, message
    - docstring
- [ ] faire les tests unitaires et les tests de validation
- [ ] modifier metric pour le rendre plus propre
- [ ] compléter le Pipfile, setup -> passer à pyproject.toml

- [ ] implémenter un système de worflows, plusieurs taches à la suite faite sur un même worker
- [ ] supprimer TaskParams pour faire une validation directement via les params fournis dans le init

- [ ] dump des schema des models en utilisant pydantic
    - https://docs.pydantic.dev/latest/concepts/serialization/

- [ ] sharded queue system
- [ ] ajouter d'autres queue type comme rabbit_mq, kafka, voir même postgres

- [ ] ajouter des tests de validation
