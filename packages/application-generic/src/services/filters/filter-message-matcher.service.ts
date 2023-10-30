import { Injectable } from '@nestjs/common';
import { FILTER_TO_LABEL, ICondition } from '@novu/shared';

import { FilterService } from './filters.service';

export interface IUsedFilters {
  filters: string[];
  failedFilters: string[];
  passedFilters: string[];
}

// TODO: Slowly move the parts of MessageMatcherUsecase (Worker app) that belong here.
@Injectable()
export class FilterMessageMatcherService extends FilterService {
  public sumFilters(summary: IUsedFilters, condition: ICondition) {
    let type: string = condition.filter?.toLowerCase();

    if (
      condition.filter === FILTER_TO_LABEL.isOnline ||
      condition.filter === FILTER_TO_LABEL.isOnlineInLast
    ) {
      type = 'online';
    }

    if (!type) {
      type = condition.filter;
    }

    type = type?.toLowerCase();

    if (condition.passed && !summary.passedFilters.includes(type)) {
      summary.passedFilters.push(type);
    }

    if (!condition.passed && !summary.failedFilters.includes(type)) {
      summary.failedFilters.push(type);
    }

    if (!summary.filters.includes(type)) {
      summary.filters.push(type);
    }

    return summary;
  }
}
