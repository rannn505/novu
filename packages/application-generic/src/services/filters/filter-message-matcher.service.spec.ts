import { FieldOperatorEnum, FILTER_TO_LABEL } from '@novu/shared';

import { FilterMessageMatcherService } from './filter-message-matcher.service';

const filterMessageMatcherService = new FilterMessageMatcherService();

describe('FilterMessageMatcherService', () => {
  describe('it summarize used filters based on condition', () => {
    it('should add a passed condition', () => {
      const result = filterMessageMatcherService.sumFilters(
        {
          filters: [],
          failedFilters: [],
          passedFilters: ['payload'],
        },
        {
          filter: FILTER_TO_LABEL.payload,
          field: '',
          expected: '',
          actual: '',
          operator: FieldOperatorEnum.LARGER,
          passed: true,
        }
      );

      expect(result.passedFilters).toContain('payload');
      expect(result.passedFilters.length).toEqual(1);
      expect(result.filters.length).toEqual(1);
      expect(result.filters).toContain('payload');
    });

    it('should add a failed condition', () => {
      const result = filterMessageMatcherService.sumFilters(
        {
          filters: [],
          failedFilters: [],
          passedFilters: [],
        },
        {
          filter: FILTER_TO_LABEL.payload,
          field: '',
          expected: '',
          actual: '',
          operator: FieldOperatorEnum.LARGER,
          passed: false,
        }
      );

      expect(result.failedFilters).toContain('payload');
      expect(result.failedFilters.length).toEqual(1);
      expect(result.filters.length).toEqual(1);
      expect(result.filters).toContain('payload');
    });

    it('should add online for both cases of online', () => {
      let result = filterMessageMatcherService.sumFilters(
        {
          filters: [],
          failedFilters: [],
          passedFilters: [],
        },
        {
          filter: FILTER_TO_LABEL.isOnlineInLast,
          field: '',
          expected: '',
          actual: '',
          operator: FieldOperatorEnum.LARGER,
          passed: true,
        }
      );

      expect(result.passedFilters).toContain('online');
      expect(result.passedFilters.length).toEqual(1);
      expect(result.filters.length).toEqual(1);
      expect(result.filters).toContain('online');

      result = filterMessageMatcherService.sumFilters(
        {
          filters: [],
          failedFilters: [],
          passedFilters: [],
        },
        {
          filter: FILTER_TO_LABEL.isOnline,
          field: '',
          expected: '',
          actual: '',
          operator: FieldOperatorEnum.LARGER,
          passed: true,
        }
      );

      expect(result.passedFilters).toContain('online');
      expect(result.passedFilters.length).toEqual(1);
      expect(result.filters.length).toEqual(1);
      expect(result.filters).toContain('online');
    });
  });
});
