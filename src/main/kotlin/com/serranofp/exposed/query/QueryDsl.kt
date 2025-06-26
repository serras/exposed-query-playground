package com.serranofp.exposed.query

import org.jetbrains.exposed.v1.core.*
import org.jetbrains.exposed.v1.dao.*
import org.jetbrains.exposed.v1.jdbc.*

@DslMarker
annotation class QueryScopeMarker

@QueryScopeMarker
class QueryScope{
    data class Result<T>(val mapper: (ResultRow) -> T)

    private val gatherers: MutableList<Gatherer> = mutableListOf()
    private sealed interface Gatherer
    private data class From(val table: ColumnSet) : Gatherer
    private data class Join(val table: ColumnSet, val joinType: JoinType, val additionalConstraint: (SqlExpressionBuilder.() -> Op<Boolean>)? = null) : Gatherer

    private val operations: MutableList<Operation> = mutableListOf()
    private data class Operation(val trasform: (Query) -> Query)

    private var selector: Selector = SelectAll
    private sealed interface Selector
    private data class SelectExpression(val expressions: List<Expression<*>>) : Selector
    private data object SelectAll : Selector

    fun <T: ColumnSet> from(table: T): T {
        gatherers.add(From(table))
        return table
    }

    fun <T: ColumnSet> join(table: T, joinType: JoinType, additionalConstraint: (SqlExpressionBuilder.(T) -> Op<Boolean>)? = null): T {
        gatherers.add(Join(table, joinType, additionalConstraint?.let { { it(table) } }))
        return table
    }

    fun <T: ColumnSet> innerJoin(table: T, additionalConstraint: (SqlExpressionBuilder.(T) -> Op<Boolean>)? = null): T =
        join(table, JoinType.INNER, additionalConstraint)

    fun using(transform: (Query) -> Query) {
        operations.add(Operation(transform))
    }

    fun where(predicate: SqlExpressionBuilder.() -> Op<Boolean>) = using { it.andWhere(predicate) }

    fun groupBy(vararg columns: Expression<*>) = using { it.groupBy(*columns) }
    fun having(predicate: SqlExpressionBuilder.() -> Op<Boolean>) = using { it.andHaving(predicate) }

    fun orderBy(vararg order: Pair<Expression<*>, SortOrder>) = using { it.orderBy(*order) }
    fun withDistinct() = using { it.withDistinct() }
    fun withDistinctOn(vararg columns: Column<*>) = using { it.withDistinctOn(*columns) }

    fun limit(count: Int) = using { it.limit(count) }
    fun offset(start: Long) = using { it.offset(start) }

    fun <T> select(expression: Expression<T>): Result<T> {
        selector = SelectExpression(listOf(expression))
        return Result { it[expression] }
    }

    fun <A, B> select(expression1: Expression<A>, expression2: Expression<B>): Result<Pair<A, B>> {
        selector = SelectExpression(listOf(expression1, expression2))
        return Result { Pair(it[expression1], it[expression2]) }
    }

    fun <Id, T: Entity<Id>, E: EntityClass<Id, T>> selectAs(entity: E): Result<T> {
        selector = SelectAll
        return Result { entity.wrapRow(it) }
    }

    fun selectRow(): Result<ResultRow> {
        selector = SelectAll
        return Result { it }
    }

    fun build(): Query {
        require(gatherers.isNotEmpty()) { "at least one 'from' is required" }
        val firstGatherer = gatherers.first()
        require(firstGatherer is From) { "at least one 'from' is required before the first 'join'" }
        var currentColumnSet = firstGatherer.table
        for (gatherer in gatherers.drop(1)) {
            currentColumnSet = when (gatherer) {
                is From -> currentColumnSet.fullJoin(gatherer.table)
                is Join -> currentColumnSet.join(gatherer.table, gatherer.joinType, additionalConstraint = gatherer.additionalConstraint)
            }
        }

        var currentQuery = when (val s = selector) {
            is SelectExpression -> {
                // prevent https://stackoverflow.com/questions/72463300/query-with-2-joins-and-subquery-using-kotlin-exposed
                var aliasCounter = 1
                val aliasedExpressions = s.expressions.map {
                    if (it is org.jetbrains.exposed.v1.core.Function<*>) it.alias("__alias${aliasCounter++}") else it
                }
                currentColumnSet.select(aliasedExpressions)
            }
            is SelectAll -> currentColumnSet.selectAll()
        }
        for (operation in operations) {
            currentQuery = operation.trasform(currentQuery)
        }

        return currentQuery
    }
}

/**
 * Return the [Query] to be executed to collect the desired information.
 **/
fun <A> subQuery(body: QueryScope.() -> QueryScope.Result<A>): Query {
    val queryScope = QueryScope()
    body(queryScope)
    return queryScope.build()
}

/**
 * Run the [Query] defined in the [body].
 * The last selector defines the form of the collected values.
 */
fun <A> query(body: QueryScope.() -> QueryScope.Result<A>): SizedIterable<A> {
    val queryScope = QueryScope()
    val selection = body(queryScope)
    return queryScope.build().mapLazy(selection.mapper)
}
