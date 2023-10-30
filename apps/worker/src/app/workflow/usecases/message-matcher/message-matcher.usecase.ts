import * as _ from 'lodash';
import axios from 'axios';
import { Injectable } from '@nestjs/common';
import { parseISO, differenceInMinutes, differenceInHours, differenceInDays } from 'date-fns';
import {
  FilterParts,
  IWebhookFilterPart,
  IRealtimeOnlineFilterPart,
  IOnlineInLastFilterPart,
  FieldLogicalOperatorEnum,
  FILTER_TO_LABEL,
  FilterPartTypeEnum,
  ICondition,
  TimeOperatorEnum,
  ChannelTypeEnum,
  IPreviousStepFilterPart,
  PreviousStepTypeEnum,
  ExecutionDetailsSourceEnum,
  ExecutionDetailsStatusEnum,
  FieldOperatorEnum,
} from '@novu/shared';
import {
  EnvironmentRepository,
  ExecutionDetailsRepository,
  JobEntity,
  JobRepository,
  MessageRepository,
  SubscriberEntity,
  SubscriberRepository,
  StepFilter,
} from '@novu/dal';
import {
  buildSubscriberKey,
  CachedEntity,
  CreateExecutionDetails,
  CreateExecutionDetailsCommand,
  DetailEnum,
  FilterMessageMatcherService,
  FilterProcessingDetails,
  IFilterVariables,
  Instrument,
  IUsedFilters,
} from '@novu/application-generic';
import { EmailEventStatusEnum } from '@novu/stateless';

import { EXCEPTION_MESSAGE_ON_WEBHOOK_FILTER, createHash, PlatformException } from '../../../shared/utils';
import { MessageMatcherCommand } from './message-matcher.command';

interface IFilterData {
  subscriber?: SubscriberEntity;
  payload?: any;
}

const differenceIn = (currentDate: Date, lastDate: Date, timeOperator: TimeOperatorEnum) => {
  if (timeOperator === TimeOperatorEnum.MINUTES) {
    return differenceInMinutes(currentDate, lastDate);
  }

  if (timeOperator === TimeOperatorEnum.HOURS) {
    return differenceInHours(currentDate, lastDate);
  }

  return differenceInDays(currentDate, lastDate);
};

@Injectable()
export class MessageMatcher {
  constructor(
    private subscriberRepository: SubscriberRepository,
    private createExecutionDetails: CreateExecutionDetails,
    private environmentRepository: EnvironmentRepository,
    public filterMessageMatcher: FilterMessageMatcherService,
    private executionDetailsRepository: ExecutionDetailsRepository,
    private messageRepository: MessageRepository,
    private jobRepository: JobRepository
  ) {}

  async execute(command: MessageMatcherCommand): Promise<{
    passed: boolean;
    data: IFilterData;
    conditions: ICondition[];
    usedFilters: IUsedFilters;
  }> {
    const data = await this.getFilterData(command);

    const { step } = command;

    let conditions: ICondition[] = [];
    if (!step?.filters || !Array.isArray(step?.filters) || step.filters.length === 0) {
      return {
        passed: true,
        data,
        conditions,
        usedFilters: this.getUsedFilters(conditions),
      };
    }

    const details: FilterProcessingDetails[] = [];
    const foundFilter = await this.filterMessageMatcher.findAsync(step.filters, async (filter) => {
      const filterProcessingDetails = new FilterProcessingDetails();
      filterProcessingDetails.addFilter(filter, data);

      const children = filter.children;
      const noRules = !children || (Array.isArray(children) && children.length === 0);
      if (noRules) {
        return true;
      }

      const singleRule = Array.isArray(children) && children.length === 1;
      const result = singleRule
        ? await this.processFilter(data, children[0], command, filterProcessingDetails)
        : await this.handleGroupFilters(filter, data, command, filterProcessingDetails);

      if (!command.prefiltering) {
        await this.sendExecutionDetails(command.job, filterProcessingDetails);
      }

      details.push(filterProcessingDetails);

      return result;
    });

    conditions = details
      .map((detail) => detail.toObject().conditions)
      .reduce((conditionsArray, collection) => [...collection, ...conditionsArray], []);

    return {
      passed: !!foundFilter,
      data,
      conditions,
      usedFilters: this.getUsedFilters(conditions),
    };
  }

  private getUsedFilters(conditions: ICondition[]): IUsedFilters {
    return conditions.reduce(this.filterMessageMatcher.sumFilters, {
      filters: [],
      failedFilters: [],
      passedFilters: [],
    });
  }

  private async sendExecutionDetails(job: JobEntity, filterProcessingDetails: FilterProcessingDetails): Promise<void> {
    await this.createExecutionDetails.execute(
      CreateExecutionDetailsCommand.create({
        ...CreateExecutionDetailsCommand.getDetailsFromJob(job),
        detail: DetailEnum.PROCESSING_STEP_FILTER,
        source: ExecutionDetailsSourceEnum.INTERNAL,
        status: ExecutionDetailsStatusEnum.PENDING,
        isTest: false,
        isRetry: false,
        raw: filterProcessingDetails.toString(),
      })
    );
  }

  private async handleGroupFilters(
    filter: StepFilter,
    variables: IFilterVariables,
    command: MessageMatcherCommand,
    filterProcessingDetails: FilterProcessingDetails
  ): Promise<boolean> {
    if (filter.value === FieldLogicalOperatorEnum.OR) {
      return await this.handleOrFilters(filter, variables, command, filterProcessingDetails);
    }

    if (filter.value === FieldLogicalOperatorEnum.AND) {
      return await this.handleAndFilters(filter, variables, command, filterProcessingDetails);
    }

    return false;
  }

  private splitFilters(filter: StepFilter) {
    const webhookFilters = filter.children.filter((childFilter) => childFilter.on === FilterPartTypeEnum.WEBHOOK);

    const otherFilters = filter.children.filter((childFilter) => childFilter.on !== FilterPartTypeEnum.WEBHOOK);

    return { webhookFilters, otherFilters };
  }

  private async handleAndFilters(
    filter: StepFilter,
    variables: IFilterVariables,
    command: MessageMatcherCommand,
    filterProcessingDetails: FilterProcessingDetails
  ): Promise<boolean> {
    const { webhookFilters, otherFilters } = this.splitFilters(filter);

    const matchedOtherFilters = await this.filterMessageMatcher.filterAsync(otherFilters, (i) =>
      this.processFilter(variables, i, command, filterProcessingDetails)
    );
    if (otherFilters.length !== matchedOtherFilters.length) {
      return false;
    }

    const matchedWebhookFilters = await this.filterMessageMatcher.filterAsync(webhookFilters, (i) =>
      this.processFilter(variables, i, command, filterProcessingDetails)
    );

    return matchedWebhookFilters.length === webhookFilters.length;
  }

  private async handleOrFilters(
    filter: StepFilter,
    variables: IFilterVariables,
    command: MessageMatcherCommand,
    filterProcessingDetails: FilterProcessingDetails
  ): Promise<boolean> {
    const { webhookFilters, otherFilters } = this.splitFilters(filter);

    const foundFilter = await this.filterMessageMatcher.findAsync(otherFilters, (i) =>
      this.processFilter(variables, i, command, filterProcessingDetails)
    );
    if (foundFilter) {
      return true;
    }

    return !!(await this.filterMessageMatcher.findAsync(webhookFilters, (i) =>
      this.processFilter(variables, i, command, filterProcessingDetails)
    ));
  }

  private async processPreviousStep(
    filter: IPreviousStepFilterPart,
    command: MessageMatcherCommand,
    filterProcessingDetails: FilterProcessingDetails
  ): Promise<boolean> {
    const job = await this.jobRepository.findOne({
      transactionId: command.transactionId,
      // backward compatibility - ternary needed to be removed once the queue renewed
      _subscriberId: command._subscriberId ? command._subscriberId : command.subscriberId,
      _environmentId: command.environmentId,
      _organizationId: command.organizationId,
      'step.uuid': filter.step,
    });

    if (!job) {
      return true;
    }

    const message = await this.messageRepository.findOne({
      _jobId: job._id,
      _environmentId: command.environmentId,
      // backward compatibility - ternary needed to be removed once the queue renewed
      _subscriberId: command._subscriberId ? command._subscriberId : command.subscriberId,
      transactionId: command.transactionId,
    });

    if (!message) {
      return true;
    }

    const label = FILTER_TO_LABEL[filter.on];
    const field = filter.stepType;
    const expected = 'true';
    const operator = FieldOperatorEnum.EQUAL;

    if (message?.channel === ChannelTypeEnum.EMAIL) {
      const count = await this.executionDetailsRepository.count({
        _jobId: command.job._parentId,
        _messageId: message._id,
        _environmentId: command.environmentId,
        webhookStatus: EmailEventStatusEnum.OPENED,
      });

      const passed = [PreviousStepTypeEnum.UNREAD, PreviousStepTypeEnum.UNSEEN].includes(filter.stepType)
        ? count === 0
        : count > 0;

      filterProcessingDetails.addCondition({
        filter: label,
        field,
        expected,
        actual: `${passed}`,
        operator,
        passed,
      });

      return passed;
    }

    const value = [PreviousStepTypeEnum.SEEN, PreviousStepTypeEnum.UNSEEN].includes(filter.stepType)
      ? message.seen
      : message.read;
    const passed = [PreviousStepTypeEnum.UNREAD, PreviousStepTypeEnum.UNSEEN].includes(filter.stepType)
      ? value === false
      : value;

    filterProcessingDetails.addCondition({
      filter: label,
      field,
      expected,
      actual: `${passed}`,
      operator,
      passed,
    });

    return passed;
  }

  private async processIsOnline(
    filter: IRealtimeOnlineFilterPart | IOnlineInLastFilterPart,
    command: MessageMatcherCommand,
    filterProcessingDetails: FilterProcessingDetails
  ): Promise<boolean> {
    const subscriber = await this.subscriberRepository.findOne({
      _id: command._subscriberId,
      _organizationId: command.organizationId,
      _environmentId: command.environmentId,
    });

    const hasNoOnlineFieldsSet =
      typeof subscriber?.isOnline === 'undefined' && typeof subscriber?.lastOnlineAt === 'undefined';
    const isOnlineString = `${subscriber?.isOnline ?? ''}`;
    const lastOnlineAtString = `${subscriber?.lastOnlineAt ?? ''}`;
    // the old subscriber created before the is online functionality should not be processed
    if (hasNoOnlineFieldsSet) {
      filterProcessingDetails.addCondition({
        filter: FILTER_TO_LABEL[filter.on],
        field: 'isOnline',
        expected: `${filter.value}`,
        actual: `${filter.on === FilterPartTypeEnum.IS_ONLINE ? isOnlineString : lastOnlineAtString}`,
        operator: filter.on === FilterPartTypeEnum.IS_ONLINE ? FieldOperatorEnum.EQUAL : filter.timeOperator,
        passed: false,
      });

      return false;
    }

    const isOnlineMatch = subscriber?.isOnline === filter.value;
    if (filter.on === FilterPartTypeEnum.IS_ONLINE) {
      filterProcessingDetails.addCondition({
        filter: FILTER_TO_LABEL[filter.on],
        field: 'isOnline',
        expected: `${filter.value}`,
        actual: isOnlineString,
        operator: FieldOperatorEnum.EQUAL,
        passed: isOnlineMatch,
      });

      return isOnlineMatch;
    }

    const currentDate = new Date();
    const lastOnlineAt = subscriber?.lastOnlineAt ? parseISO(subscriber?.lastOnlineAt) : new Date();
    const diff = differenceIn(currentDate, lastOnlineAt, filter.timeOperator);
    const result = subscriber?.isOnline || (!subscriber?.isOnline && diff >= 0 && diff <= filter.value);

    filterProcessingDetails.addCondition({
      filter: FILTER_TO_LABEL[filter.on],
      field: subscriber?.isOnline ? 'isOnline' : 'lastOnlineAt',
      expected: subscriber?.isOnline ? 'true' : `${filter.value}`,
      actual: `${subscriber?.isOnline ? 'true' : diff}`,
      operator: filter.timeOperator,
      passed: result,
    });

    return result;
  }

  private async getWebhookResponse(
    child: IWebhookFilterPart,
    variables: IFilterVariables,
    command: MessageMatcherCommand
  ): Promise<Record<string, unknown> | undefined> {
    if (!child.webhookUrl) return undefined;

    const payload = await this.buildPayload(variables, command);

    const hmac = await this.buildHmac(command);

    const config = {
      headers: {
        'nv-hmac-256': hmac,
      },
    };

    try {
      return await axios.post(child.webhookUrl, payload, config).then((response) => {
        return response.data as Record<string, unknown>;
      });
    } catch (err) {
      throw new Error(
        JSON.stringify({
          message: err.message,
          data: EXCEPTION_MESSAGE_ON_WEBHOOK_FILTER,
        })
      );
    }
  }

  private async buildPayload(variables: IFilterVariables, command: MessageMatcherCommand) {
    if (process.env.NODE_ENV === 'test') return variables;

    const payload: Partial<{
      subscriber: SubscriberEntity | null;
      payload: Record<string, unknown>;
      identifier: string;
      channel: string;
      providerId: string;
    }> = {};

    if (variables.subscriber) {
      payload.subscriber = variables.subscriber;
    } else {
      payload.subscriber = await this.subscriberRepository.findBySubscriberId(
        command.environmentId,
        command.subscriberId
      );
    }

    if (variables.payload) {
      payload.payload = variables.payload;
    }

    payload.identifier = command.identifier;
    payload.channel = command.job.type;

    if (command.job.providerId) {
      payload.providerId = command.job.providerId;
    }

    return payload;
  }

  private async buildHmac(command: MessageMatcherCommand): Promise<string> {
    if (process.env.NODE_ENV === 'test') return '';

    const environment = await this.environmentRepository.findOne({
      _id: command.environmentId,
      _organizationId: command.organizationId,
    });
    if (!environment) throw new PlatformException('Environment is not found');

    return createHash(environment.apiKeys[0].key, command.environmentId);
  }

  private async processFilter(
    variables: IFilterVariables,
    child: FilterParts,
    command: MessageMatcherCommand,
    filterProcessingDetails: FilterProcessingDetails
  ): Promise<boolean> {
    let passed = false;

    if (child.on === FilterPartTypeEnum.WEBHOOK) {
      if (process.env.NODE_ENV === 'test') return true;

      const res = await this.getWebhookResponse(child, variables, command);
      passed = this.filterMessageMatcher.processFilterEquality(
        { payload: undefined, webhook: res },
        child,
        filterProcessingDetails
      );
    }

    if (child.on === FilterPartTypeEnum.PAYLOAD || child.on === FilterPartTypeEnum.SUBSCRIBER) {
      passed = this.filterMessageMatcher.processFilterEquality(variables, child, filterProcessingDetails);
    }

    if (child.on === FilterPartTypeEnum.IS_ONLINE || child.on === FilterPartTypeEnum.IS_ONLINE_IN_LAST) {
      passed = await this.processIsOnline(child, command, filterProcessingDetails);
    }

    if (child.on === FilterPartTypeEnum.PREVIOUS_STEP) {
      passed = await this.processPreviousStep(child, command, filterProcessingDetails);
    }

    return passed;
  }

  @Instrument()
  public async getFilterData(command: MessageMatcherCommand): Promise<IFilterData> {
    const subscriberFilterExist = command.step?.filters?.find((filter) => {
      return filter?.children?.find((item) => item?.on === 'subscriber');
    });

    let subscriber;

    if (subscriberFilterExist) {
      subscriber = await this.getSubscriberBySubscriberId({
        subscriberId: command.subscriberId,
        _environmentId: command.environmentId,
      });
    }

    return {
      subscriber,
      payload: command.job.payload,
    };
  }

  @CachedEntity({
    builder: (command: { subscriberId: string; _environmentId: string }) =>
      buildSubscriberKey({
        _environmentId: command._environmentId,
        subscriberId: command.subscriberId,
      }),
  })
  public async getSubscriberBySubscriberId({
    subscriberId,
    _environmentId,
  }: {
    subscriberId: string;
    _environmentId: string;
  }) {
    return await this.subscriberRepository.findOne({
      _environmentId,
      subscriberId,
    });
  }
}
