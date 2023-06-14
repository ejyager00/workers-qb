import { QueryBuilder } from '../Builder'
import { FetchTypes } from '../enums'
import { Raw } from '../tools'
import { D1Result, D1ResultOne } from '../interfaces'

export class D1QB extends QueryBuilder<D1Result, D1ResultOne> {
  private db: any

  constructor(db: any) {
    super()
    this.db = db
  }

  async execute(params: {
    query: string
    arguments?: (string | number | boolean | null | Raw)[]
    fetchType?: FetchTypes
  }): Promise<any> {
    let stmt = this.db.prepare(params.query)

    if (this._debugger) {
      console.log({
        'workers-qb': params,
      })
    }

    if (params.arguments) {
      stmt = stmt.bind(...params.arguments)
    }

    if (params.fetchType === FetchTypes.ONE || params.fetchType === FetchTypes.ALL) {
      const resp = await stmt.all()

      return {
        changes: resp.meta?.changes,
        duration: resp.meta?.duration,
        last_row_id: resp.meta?.last_row_id,
        served_by: resp.meta?.served_by,
        success: resp.success,
        results: params.fetchType === FetchTypes.ONE && resp.results.length > 0 ? resp.results[0] : resp.results,
      }
    }

    return stmt.run()
  }

  async batchExecute(
    params: [
      {
        query: string
        arguments?: (string | number | boolean | Raw | null)[] | undefined
        fetchType?: FetchTypes | undefined
      }
    ]
  ): Promise<any> {
    if (this._debugger) {
      console.log({
        'workers-qb': params,
      })
    }

    const resp = await this.db.batch(
      params.map((param) => {
        let argument_arr = param.arguments ? param.arguments : []
        return this.db.prepare(param.query).bind(...argument_arr)
      })
    )

    return resp.map(
      (
        r: {
          meta: { changes: any; duration: any; last_row_id: any; served_by: any }
          success: any
          results: string | any[]
        },
        i: number
      ) => {
        return {
          changes: r.meta?.changes,
          duration: r.meta?.duration,
          last_row_id: r.meta?.last_row_id,
          served_by: r.meta?.served_by,
          success: r.success,
          results: params[i].fetchType === FetchTypes.ONE && r.results.length > 0 ? r.results[0] : r.results,
        }
      }
    )
  }
}
