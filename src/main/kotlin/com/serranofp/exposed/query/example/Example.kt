package com.serranofp.exposed.query.example

import com.serranofp.exposed.query.query
import org.jetbrains.exposed.v1.core.SortOrder
import org.jetbrains.exposed.v1.core.count
import org.jetbrains.exposed.v1.core.dao.id.EntityID
import org.jetbrains.exposed.v1.core.dao.id.IntIdTable
import org.jetbrains.exposed.v1.dao.IntEntity
import org.jetbrains.exposed.v1.dao.IntEntityClass

object StarWarsFilmsTable : IntIdTable() {
    val sequelId = integer("sequel_id").uniqueIndex()
    val name = varchar("name", 100)
    val director = varchar("director", 100)
}

class StarWarsFilmEntity(id: EntityID<Int>) : IntEntity(id) {
    companion object : IntEntityClass<StarWarsFilmEntity>(StarWarsFilmsTable)

    var sequelId by StarWarsFilmsTable.sequelId
    var name by StarWarsFilmsTable.name
    var director by StarWarsFilmsTable.director
}

object ActorsIntIdTable : IntIdTable("actors") {
    val sequelId = integer("sequel_id").uniqueIndex()
    val name = varchar("name", 50)
}

object RolesTable : IntIdTable() {
    val sequelId = integer("sequel_id")
    val actorId = reference("actor_id", ActorsIntIdTable)
    val characterName = varchar("name", 50)
}

val exampleQuery1a = query {
    val film = from(StarWarsFilmsTable)
    withDistinctOn(film.sequelId)
    orderBy(film.sequelId to SortOrder.ASC)
    select(film.director, film.name)
}

val exampleQuery1b = query {
    val film = from(StarWarsFilmsTable)
    withDistinctOn(film.sequelId)
    orderBy(film.sequelId to SortOrder.ASC)
    selectAs(StarWarsFilmEntity)
}

/*
ActorsIntIdTable.join(RolesTable, JoinType.INNER, onColumn = ActorsIntIdTable.id, otherColumn = RolesTable.actorId)
    .select(RolesTable.characterName.count(), ActorsIntIdTable.name)
    .groupBy(ActorsIntIdTable.name)
    .toList()
*/
val exampleQuery2 = query {
    val actor = from(ActorsIntIdTable)
    val role = innerJoin(RolesTable) { actor.id eq it.actorId }
    groupBy(actor.name)
    select(role.characterName.count(), actor.name)
}

/*
StarWarsFilmsIntIdTable
    .selectAll()
    .withDistinct()
    .where { StarWarsFilmsIntIdTable.sequelId less MOVIE_SEQUEL_ID }
    .groupBy(StarWarsFilmsIntIdTable.id)
 */
val exampleQuery3 = query {
    val film = from(StarWarsFilmsTable)
    withDistinct()
    where { film.sequelId less 10 }
    groupBy(film.id)
    selectRow()
}