import { expect } from 'chai';
import * as sinon from 'sinon';
import axios from 'axios';
import { Duration, sub } from 'date-fns';
import {
  BuilderGroupValues,
  FieldLogicalOperatorEnum,
  FieldOperatorEnum,
  FilterParts,
  FilterPartTypeEnum,
  FILTER_TO_LABEL,
  StepTypeEnum,
  TimeOperatorEnum,
} from '@novu/shared';
import {
  EnvironmentRepository,
  ExecutionDetailsRepository,
  JobEntity,
  JobRepository,
  MessageRepository,
  MessageTemplateEntity,
  NotificationStepEntity,
  SubscriberRepository,
} from '@novu/dal';
import { CreateExecutionDetails, FilterMessageMatcherService, IFilterVariables } from '@novu/application-generic';

import { MessageMatcher } from './message-matcher.usecase';
import type { SendMessageCommand } from '../send-message/send-message.command';

let getFilterDataStub;
let messageMatcher;

describe('Message filter matcher', function () {
  describe('Expected functionalities', () => {
    beforeEach(() => {
      messageMatcher = buildMessageMatcher(new SubscriberRepository());
      getFilterDataStub = sinon.stub(messageMatcher, 'getFilterData');
    });

    afterEach(() => {
      getFilterDataStub.restore();
    });

    it('should filter correct message by the filter value', async function () {
      const payload = {
        varField: 'firstVar',
      };

      getFilterDataStub.resolves({
        payload,
      });

      const matchedMessage = await messageMatcher.execute(
        sendMessageCommand({
          step: makeStep('Correct Match', FieldLogicalOperatorEnum.OR, [
            {
              operator: FieldOperatorEnum.EQUAL,
              value: 'firstVar',
              field: 'varField',
              on: FilterPartTypeEnum.PAYLOAD,
            },
          ]),
        })
      );

      expect(matchedMessage.passed).to.equal(true);
    });

    it('should match a message for AND filter group', async function () {
      const payload = {
        varField: 'firstVar',
        secondField: 'secondVar',
      };
      getFilterDataStub.resolves({
        payload,
      });

      const matchedMessage = await messageMatcher.execute(
        sendMessageCommand({
          step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
            {
              operator: FieldOperatorEnum.EQUAL,
              value: 'firstVar',
              field: 'varField',
              on: FilterPartTypeEnum.PAYLOAD,
            },
            {
              operator: FieldOperatorEnum.EQUAL,
              value: 'secondVar',
              field: 'secondField',
              on: FilterPartTypeEnum.PAYLOAD,
            },
          ]),
        })
      );

      expect(matchedMessage.passed).to.equal(true);
    });

    it('should not match AND group for single bad item', async function () {
      const payload = {
        varField: 'firstVar',
        secondField: 'secondVarBad',
      };
      getFilterDataStub.resolves({
        payload,
      });

      const matchedMessage = await messageMatcher.execute(
        sendMessageCommand({
          step: makeStep('Title', FieldLogicalOperatorEnum.AND, [
            {
              operator: FieldOperatorEnum.EQUAL,
              value: 'firstVar',
              field: 'varField',
              on: FilterPartTypeEnum.PAYLOAD,
            },
            {
              operator: FieldOperatorEnum.EQUAL,
              value: 'secondVar',
              field: 'secondField',
              on: FilterPartTypeEnum.PAYLOAD,
            },
          ]),
        })
      );

      expect(matchedMessage.passed).to.equal(false);
    });

    it('should match a NOT_EQUAL for EQUAL var', async function () {
      const payload = {
        varField: 'firstVar',
        secondField: 'secondVarBad',
      };
      getFilterDataStub.resolves({
        payload,
      });

      const matchedMessage = await messageMatcher.execute(
        sendMessageCommand({
          step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
            {
              operator: FieldOperatorEnum.EQUAL,
              value: 'firstVar',
              field: 'varField',
              on: FilterPartTypeEnum.PAYLOAD,
            },
            {
              operator: FieldOperatorEnum.NOT_EQUAL,
              value: 'secondVar',
              field: 'secondField',
              on: FilterPartTypeEnum.PAYLOAD,
            },
          ]),
        })
      );

      expect(matchedMessage.passed).to.equal(true);
    });

    it('should match a EQUAL for a boolean var', async function () {
      const payload = {
        varField: true,
      };
      getFilterDataStub.resolves({
        payload,
      });

      const matchedMessage = await messageMatcher.execute(
        sendMessageCommand({
          step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
            {
              operator: FieldOperatorEnum.EQUAL,
              value: 'true',
              field: 'varField',
              on: FilterPartTypeEnum.PAYLOAD,
            },
          ]),
        })
      );

      expect(matchedMessage.passed).to.equal(true);
    });

    it('should fall thru for no filters item', async function () {
      const payload = {
        varField: 'firstVar',
        secondField: 'secondVarBad',
      };
      getFilterDataStub.resolves({
        payload,
      });

      const matchedMessage = await messageMatcher.execute(
        sendMessageCommand({ step: makeStep('Correct Match 2', FieldLogicalOperatorEnum.OR, []) })
      );

      expect(matchedMessage.passed).to.equal(true);
    });

    it('should get larger payload var then filter value', async function () {
      const payload = {
        varField: 3,
      };
      getFilterDataStub.resolves({
        payload,
      });

      const matchedMessage = await messageMatcher.execute(
        sendMessageCommand({
          step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
            {
              operator: FieldOperatorEnum.LARGER,
              value: '0',
              field: 'varField',
              on: FilterPartTypeEnum.PAYLOAD,
            },
          ]),
        })
      );

      expect(matchedMessage.passed).to.equal(true);
    });

    it('should get smaller payload var then filter value', async function () {
      const payload = {
        varField: 0,
      };
      getFilterDataStub.resolves({
        payload,
      });

      const matchedMessage = await messageMatcher.execute(
        sendMessageCommand({
          step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
            {
              operator: FieldOperatorEnum.SMALLER,
              value: '3',
              field: 'varField',
              on: FilterPartTypeEnum.PAYLOAD,
            },
          ]),
        })
      );

      expect(matchedMessage.passed).to.equal(true);
    });

    it('should get larger or equal payload var then filter value', async function () {
      let payload = {
        varField: 3,
      };
      getFilterDataStub.resolves({
        payload,
      });

      let matchedMessage = await messageMatcher.execute(
        sendMessageCommand({
          step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
            {
              operator: FieldOperatorEnum.LARGER_EQUAL,
              value: '0',
              field: 'varField',
              on: FilterPartTypeEnum.PAYLOAD,
            },
          ]),
        })
      );

      expect(matchedMessage.passed).to.equal(true);

      payload = {
        varField: 3,
      };
      getFilterDataStub.resolves({
        payload,
      });

      matchedMessage = await messageMatcher.execute(
        sendMessageCommand({
          step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
            {
              operator: FieldOperatorEnum.LARGER_EQUAL,
              value: '3',
              field: 'varField',
              on: FilterPartTypeEnum.PAYLOAD,
            },
          ]),
        })
      );

      expect(matchedMessage.passed).to.equal(true);
    });

    it('should check if value is defined in payload', async function () {
      const payload = {
        emailMessage: '<b>This works</b>',
      };
      getFilterDataStub.resolves({
        payload,
      });

      const matchedMessage = await messageMatcher.execute(
        sendMessageCommand({
          step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
            {
              operator: FieldOperatorEnum.IS_DEFINED,
              value: '',
              field: 'emailMessage',
              on: FilterPartTypeEnum.PAYLOAD,
            },
          ]),
        })
      );

      expect(matchedMessage.passed).to.equal(true);
    });

    it('should check if key is defined or not in subscriber data', async function () {
      const subscriber = {
        firstName: '',
        lastName: '',
        email: '',
        subscriberId: '',
        deleted: false,
        createdAt: '',
        updatedAt: '',
        _id: '',
        _organizationId: '',
        _environmentId: '',
        data: {
          nested_Key: 'nestedValue',
        },
      };
      getFilterDataStub.resolves({
        subscriber,
      });

      const matchedMessage = await messageMatcher.execute(
        sendMessageCommand({
          step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
            {
              operator: FieldOperatorEnum.IS_DEFINED,
              value: '',
              field: 'data.nestedKey',
              on: FilterPartTypeEnum.SUBSCRIBER,
            },
          ]),
        })
      );

      expect(matchedMessage.passed).to.equal(false);
    });

    it('should get nested custom subscriber data', async function () {
      const subscriber = {
        firstName: '',
        lastName: '',
        email: '',
        subscriberId: '',
        deleted: false,
        createdAt: '',
        updatedAt: '',
        _id: '',
        _organizationId: '',
        _environmentId: '',
        data: {
          nestedKey: 'nestedValue',
        },
      };
      getFilterDataStub.resolves({
        subscriber,
      });

      const matchedMessage = await messageMatcher.execute(
        sendMessageCommand({
          step: makeStep('Correct Match', FieldLogicalOperatorEnum.OR, [
            {
              operator: FieldOperatorEnum.EQUAL,
              value: 'nestedValue',
              field: 'data.nestedKey',
              on: FilterPartTypeEnum.SUBSCRIBER,
            },
          ]),
        })
      );

      expect(matchedMessage.passed).to.equal(true);
    });

    it(`should return false with nested data that doesn't exist`, async function () {
      const payload = {
        data: {
          nestedKey: {
            childKey: 'nestedValue',
          },
        },
      };
      getFilterDataStub.resolves({
        payload,
      });

      const matchedMessage = await messageMatcher.execute(
        sendMessageCommand({
          step: makeStep('Correct Match', FieldLogicalOperatorEnum.OR, [
            {
              operator: FieldOperatorEnum.EQUAL,
              value: 'nestedValue',
              field: 'data.nestedKey.doesNotExist',
              on: FilterPartTypeEnum.PAYLOAD,
            },
          ]),
        })
      );

      expect(matchedMessage.passed).to.equal(false);
    });

    it('should get smaller or equal payload var then filter value', async function () {
      let payload = {
        varField: 0,
      };
      getFilterDataStub.resolves({
        payload,
      });

      let matchedMessage = await messageMatcher.execute(
        sendMessageCommand({
          step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
            {
              operator: FieldOperatorEnum.SMALLER_EQUAL,
              value: '3',
              field: 'varField',
              on: FilterPartTypeEnum.PAYLOAD,
            },
          ]),
        })
      );

      expect(matchedMessage.passed).to.equal(true);

      payload = {
        varField: 3,
      };
      getFilterDataStub.resolves({
        payload,
      });

      matchedMessage = await messageMatcher.execute(
        sendMessageCommand({
          step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
            {
              operator: FieldOperatorEnum.SMALLER_EQUAL,
              value: '3',
              field: 'varField',
              on: FilterPartTypeEnum.PAYLOAD,
            },
          ]),
        })
      );

      expect(matchedMessage.passed).to.equal(true);
    });

    it('should handle now filters', async function () {
      let payload = {
        varField: 3,
      };
      getFilterDataStub.resolves({
        payload,
      });

      let matchedMessage = await messageMatcher.execute(
        sendMessageCommand({
          step: {
            _templateId: '123',
            template: {
              subject: 'Test Subject',
              type: StepTypeEnum.EMAIL,
              name: '',
              content: 'Test',
              _organizationId: '123',
              _environmentId: 'asdas',
              _creatorId: '123',
            } as MessageTemplateEntity,
            filters: undefined,
          },
        })
      );
      expect(matchedMessage.passed).to.equal(true);

      payload = {
        varField: 3,
      };
      getFilterDataStub.resolves({
        payload,
      });

      matchedMessage = await messageMatcher.execute(
        sendMessageCommand({
          step: {
            _templateId: '123',
            template: {
              subject: 'Test Subject',
              type: StepTypeEnum.EMAIL,
              name: '',
              content: 'Test',
              _organizationId: '123',
              _environmentId: 'asdas',
              _creatorId: '123',
            } as MessageTemplateEntity,
            filters: [],
          },
        })
      );
      expect(matchedMessage.passed).to.equal(true);

      payload = {
        varField: 3,
      };
      getFilterDataStub.resolves({
        payload,
      });

      matchedMessage = await messageMatcher.execute(
        sendMessageCommand({
          step: {
            _templateId: '123',
            template: {
              subject: 'Test Subject',
              type: StepTypeEnum.EMAIL,
              name: '',
              content: 'Test',
              _organizationId: '123',
              _environmentId: 'asdas',
              _creatorId: '123',
            } as MessageTemplateEntity,
            filters: [
              {
                isNegated: false,
                type: 'GROUP',
                value: FieldLogicalOperatorEnum.AND,
                children: [],
              },
            ],
          },
        })
      );
      expect(matchedMessage.passed).to.equal(true);

      payload = {
        varField: 3,
      };
      getFilterDataStub.resolves({
        payload,
      });

      matchedMessage = await messageMatcher.execute(
        sendMessageCommand({
          step: {
            _templateId: '123',
            template: {
              subject: 'Test Subject',
              type: StepTypeEnum.EMAIL,
              name: '',
              content: 'Test',
              _organizationId: '123',
              _environmentId: 'asdas',
              _creatorId: '123',
            } as MessageTemplateEntity,
            filters: [
              {
                isNegated: false,
                type: 'GROUP',
                value: FieldLogicalOperatorEnum.AND,
                children: [],
              },
            ],
          },
        })
      );
      expect(matchedMessage.passed).to.equal(true);
    });

    it('should handle webhook filter', async function () {
      const payload = {};
      getFilterDataStub.resolves({
        payload,
      });

      const gotGetStub = sinon.stub(axios, 'post').resolves(
        Promise.resolve({
          data: { varField: true },
        })
      );

      const matchedMessage = await messageMatcher.execute(
        sendMessageCommand({
          step: makeStep('Correct Match', undefined, [
            {
              operator: FieldOperatorEnum.EQUAL,
              value: 'true',
              field: 'varField',
              on: FilterPartTypeEnum.WEBHOOK,
              webhookUrl: 'www.user.com/webhook',
            },
          ]),
        })
      );

      expect(matchedMessage.passed).to.equal(true);

      gotGetStub.restore();
    });

    it('should skip async filter if child under OR returned true', async function () {
      let payload = { payloadVarField: true };
      getFilterDataStub.resolves({
        payload,
      });

      const gotGetStub = sinon.stub(axios, 'post').resolves(
        Promise.resolve({
          body: JSON.stringify({ varField: true }),
        })
      );

      let matchedMessage = await messageMatcher.execute(
        sendMessageCommand({
          step: makeStep('Correct Match', FieldLogicalOperatorEnum.OR, [
            {
              operator: FieldOperatorEnum.EQUAL,
              value: 'true',
              field: 'payloadVarField',
              on: FilterPartTypeEnum.PAYLOAD,
            },
            {
              operator: FieldOperatorEnum.EQUAL,
              value: 'true',
              field: 'varField',
              on: FilterPartTypeEnum.WEBHOOK,
              webhookUrl: 'www.user.com/webhook',
            },
          ]),
        })
      );

      let requestsCount = gotGetStub.callCount;

      expect(requestsCount).to.equal(0);
      expect(matchedMessage.passed).to.equal(true);

      //Reorder children order to make sure it is not random
      payload = { payloadVarField: true };
      getFilterDataStub.resolves({
        payload,
      });

      matchedMessage = await messageMatcher.execute(
        sendMessageCommand({
          step: makeStep('Correct Match', FieldLogicalOperatorEnum.OR, [
            {
              operator: FieldOperatorEnum.EQUAL,
              value: 'true',
              field: 'varField',
              on: FilterPartTypeEnum.WEBHOOK,
              webhookUrl: 'www.user.com/webhook',
            },
            {
              operator: FieldOperatorEnum.EQUAL,
              value: 'true',
              field: 'payloadVarField',
              on: FilterPartTypeEnum.PAYLOAD,
            },
          ]),
        })
      );

      requestsCount = gotGetStub.callCount;

      expect(requestsCount).to.equal(0);
      expect(matchedMessage.passed).to.equal(true);

      gotGetStub.restore();
    });

    it('should skip async filter if child under AND returned false', async function () {
      const gotGetStub = sinon.stub(axios, 'post').resolves(
        Promise.resolve({
          body: JSON.stringify({ varField: true }),
        })
      );

      let payload = { payloadVarField: false };
      getFilterDataStub.resolves({
        payload,
      });

      let matchedMessage = await messageMatcher.execute(
        sendMessageCommand({
          step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
            {
              operator: FieldOperatorEnum.EQUAL,
              value: 'true',
              field: 'payloadVarField',
              on: FilterPartTypeEnum.PAYLOAD,
            },
            {
              operator: FieldOperatorEnum.EQUAL,
              value: 'true',
              field: 'varField',
              on: FilterPartTypeEnum.WEBHOOK,
              webhookUrl: 'www.user.com/webhook',
            },
          ]),
        })
      );

      let requestsCount = gotGetStub.callCount;

      expect(requestsCount).to.equal(0);
      expect(matchedMessage.passed).to.equal(false);

      //Reorder children order to make sure it is not random
      payload = { payloadVarField: false };
      getFilterDataStub.resolves({
        payload,
      });

      matchedMessage = await messageMatcher.execute(
        sendMessageCommand({
          step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
            {
              operator: FieldOperatorEnum.EQUAL,
              value: 'true',
              field: 'varField',
              on: FilterPartTypeEnum.WEBHOOK,
              webhookUrl: 'www.user.com/webhook',
            },
            {
              operator: FieldOperatorEnum.EQUAL,
              value: 'true',
              field: 'payloadVarField',
              on: FilterPartTypeEnum.PAYLOAD,
            },
          ]),
        })
      );

      requestsCount = gotGetStub.callCount;

      expect(requestsCount).to.equal(0);
      expect(matchedMessage.passed).to.equal(false);

      gotGetStub.restore();
    });
  });

  describe('is online filters', () => {
    afterEach(() => {
      getFilterDataStub.restore();
    });

    describe('isOnline', () => {
      it('allows to process multiple filter parts', async () => {
        const mockSubscriberRepository = {
          findOne: () => Promise.resolve(getSubscriber({ isOnline: true })),
        } as unknown as SubscriberRepository;
        messageMatcher = buildMessageMatcher(mockSubscriberRepository);
        getFilterDataStub = sinon.stub(messageMatcher, 'getFilterData');

        const payload = { payloadVarField: true };
        getFilterDataStub.resolves({
          payload,
        });

        const matchedMessage = await messageMatcher.execute(
          sendMessageCommand({
            step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
              {
                on: FilterPartTypeEnum.IS_ONLINE,
                value: true,
              },
              {
                operator: FieldOperatorEnum.EQUAL,
                value: 'true',
                field: 'payloadVarField',
                on: FilterPartTypeEnum.PAYLOAD,
              },
            ]),
          })
        );
        expect(matchedMessage.passed).to.equal(true);
      });

      it(`doesn't allow to process if the subscriber has no online fields set and filter is true`, async () => {
        const mockSubscriberRepository = {
          findOne: () => Promise.resolve(getSubscriber()),
        } as unknown as SubscriberRepository;
        const payload = {};

        messageMatcher = buildMessageMatcher(mockSubscriberRepository);
        getFilterDataStub = sinon.stub(messageMatcher, 'getFilterData');
        getFilterDataStub.resolves({
          payload,
        });

        const matchedMessage = await messageMatcher.execute(
          sendMessageCommand({
            step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
              {
                on: FilterPartTypeEnum.IS_ONLINE,
                value: true,
              },
            ]),
          })
        );
        expect(matchedMessage.passed).to.equal(false);
      });

      it(`doesn't allow to process if the subscriber has no online fields set and filter is false`, async () => {
        const mockSubscriberRepository = {
          findOne: () => Promise.resolve(getSubscriber()),
        } as unknown as SubscriberRepository;
        const payload = {};

        messageMatcher = buildMessageMatcher(mockSubscriberRepository);
        getFilterDataStub = sinon.stub(messageMatcher, 'getFilterData');
        getFilterDataStub.resolves({
          payload,
        });

        const matchedMessage = await messageMatcher.execute(
          sendMessageCommand({
            step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
              {
                on: FilterPartTypeEnum.IS_ONLINE,
                value: false,
              },
            ]),
          })
        );
        expect(matchedMessage.passed).to.equal(false);
      });

      it('allows to process if the subscriber is online', async () => {
        const mockSubscriberRepository = {
          findOne: () => Promise.resolve(getSubscriber({ isOnline: true })),
        } as unknown as SubscriberRepository;
        const payload = {};

        messageMatcher = buildMessageMatcher(mockSubscriberRepository);
        getFilterDataStub = sinon.stub(messageMatcher, 'getFilterData');
        getFilterDataStub.resolves({
          payload,
        });

        const matchedMessage = await messageMatcher.execute(
          sendMessageCommand({
            step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
              {
                on: FilterPartTypeEnum.IS_ONLINE,
                value: true,
              },
            ]),
          })
        );
        expect(matchedMessage.passed).to.equal(true);
      });

      it(`doesn't allow to process if the subscriber is not online`, async () => {
        const mockSubscriberRepository = {
          findOne: () => Promise.resolve(getSubscriber({ isOnline: false })),
        } as unknown as SubscriberRepository;
        const payload = {};

        messageMatcher = buildMessageMatcher(mockSubscriberRepository);
        getFilterDataStub = sinon.stub(messageMatcher, 'getFilterData');
        getFilterDataStub.resolves({
          payload,
        });

        const matchedMessage = await messageMatcher.execute(
          sendMessageCommand({
            step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
              {
                on: FilterPartTypeEnum.IS_ONLINE,
                value: true,
              },
            ]),
          })
        );
        expect(matchedMessage.passed).to.equal(false);
      });
    });

    describe('isOnlineInLast', () => {
      it('allows to process multiple filter parts', async () => {
        const mockSubscriberRepository = {
          findOne: () => Promise.resolve(getSubscriber({ isOnline: true }, { subDuration: { minutes: 3 } })),
        } as unknown as SubscriberRepository;
        const payload = { payloadVarField: true };

        messageMatcher = buildMessageMatcher(mockSubscriberRepository);
        getFilterDataStub = sinon.stub(messageMatcher, 'getFilterData');
        getFilterDataStub.resolves({
          payload,
        });

        const matchedMessage = await messageMatcher.execute(
          sendMessageCommand({
            step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
              {
                on: FilterPartTypeEnum.IS_ONLINE_IN_LAST,
                value: 5,
                timeOperator: TimeOperatorEnum.MINUTES,
              },
              {
                operator: FieldOperatorEnum.EQUAL,
                value: 'true',
                field: 'payloadVarField',
                on: FilterPartTypeEnum.PAYLOAD,
              },
            ]),
          })
        );
        expect(matchedMessage.passed).to.equal(true);
      });

      it(`doesn't allow to process if the subscriber with no online fields set`, async () => {
        const mockSubscriberRepository = {
          findOne: () => Promise.resolve(getSubscriber()),
        } as unknown as SubscriberRepository;
        const payload = {};

        messageMatcher = buildMessageMatcher(mockSubscriberRepository);
        getFilterDataStub = sinon.stub(messageMatcher, 'getFilterData');
        getFilterDataStub.resolves({
          payload,
        });

        const matchedMessage = await messageMatcher.execute(
          sendMessageCommand({
            step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
              {
                on: FilterPartTypeEnum.IS_ONLINE_IN_LAST,
                value: 5,
                timeOperator: TimeOperatorEnum.MINUTES,
              },
            ]),
          })
        );
        expect(matchedMessage.passed).to.equal(false);
      });

      it('allows to process if the subscriber is still online', async () => {
        const mockSubscriberRepository = {
          findOne: () => Promise.resolve(getSubscriber({ isOnline: true }, { subDuration: { minutes: 10 } })),
        } as unknown as SubscriberRepository;
        const payload = {};

        messageMatcher = buildMessageMatcher(mockSubscriberRepository);
        getFilterDataStub = sinon.stub(messageMatcher, 'getFilterData');
        getFilterDataStub.resolves({
          payload,
        });

        const matchedMessage = await messageMatcher.execute(
          sendMessageCommand({
            step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
              {
                on: FilterPartTypeEnum.IS_ONLINE_IN_LAST,
                value: 5,
                timeOperator: TimeOperatorEnum.MINUTES,
              },
            ]),
          })
        );
        expect(matchedMessage.passed).to.equal(true);
      });

      it('allows to process if the subscriber was online in last 5 min', async () => {
        const mockSubscriberRepository = {
          findOne: () => Promise.resolve(getSubscriber({ isOnline: false }, { subDuration: { minutes: 4 } })),
        } as unknown as SubscriberRepository;
        const payload = {};

        messageMatcher = buildMessageMatcher(mockSubscriberRepository);
        getFilterDataStub = sinon.stub(messageMatcher, 'getFilterData');
        getFilterDataStub.resolves({
          payload,
        });

        const matchedMessage = await messageMatcher.execute(
          sendMessageCommand({
            step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
              {
                on: FilterPartTypeEnum.IS_ONLINE_IN_LAST,
                value: 5,
                timeOperator: TimeOperatorEnum.MINUTES,
              },
            ]),
          })
        );
        expect(matchedMessage.passed).to.equal(true);
      });

      it(`doesn't allow to process if the subscriber was online more that last 5 min`, async () => {
        const mockSubscriberRepository = {
          findOne: () => Promise.resolve(getSubscriber({ isOnline: false }, { subDuration: { minutes: 6 } })),
        } as unknown as SubscriberRepository;
        const payload = {};

        messageMatcher = buildMessageMatcher(mockSubscriberRepository);
        getFilterDataStub = sinon.stub(messageMatcher, 'getFilterData');
        getFilterDataStub.resolves({
          payload,
        });

        const matchedMessage = await messageMatcher.execute(
          sendMessageCommand({
            step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
              {
                on: FilterPartTypeEnum.IS_ONLINE_IN_LAST,
                value: 5,
                timeOperator: TimeOperatorEnum.MINUTES,
              },
            ]),
          })
        );
        expect(matchedMessage.passed).to.equal(false);
      });

      it('allows to process if the subscriber was online in last 1 hour', async () => {
        const mockSubscriberRepository = {
          findOne: () => Promise.resolve(getSubscriber({ isOnline: false }, { subDuration: { minutes: 30 } })),
        } as unknown as SubscriberRepository;
        const payload = {};

        messageMatcher = buildMessageMatcher(mockSubscriberRepository);
        getFilterDataStub = sinon.stub(messageMatcher, 'getFilterData');
        getFilterDataStub.resolves({
          payload,
        });

        const matchedMessage = await messageMatcher.execute(
          sendMessageCommand({
            step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
              {
                on: FilterPartTypeEnum.IS_ONLINE_IN_LAST,
                value: 1,
                timeOperator: TimeOperatorEnum.HOURS,
              },
            ]),
          })
        );
        expect(matchedMessage.passed).to.equal(true);
      });

      it('allows to process if the subscriber was online in last 1 day', async () => {
        const mockSubscriberRepository = {
          findOne: () => Promise.resolve(getSubscriber({ isOnline: false }, { subDuration: { hours: 23 } })),
        } as unknown as SubscriberRepository;
        const payload = {};

        messageMatcher = buildMessageMatcher(mockSubscriberRepository);
        getFilterDataStub = sinon.stub(messageMatcher, 'getFilterData');
        getFilterDataStub.resolves({
          payload,
        });

        const matchedMessage = await messageMatcher.execute(
          sendMessageCommand({
            step: makeStep('Correct Match', FieldLogicalOperatorEnum.AND, [
              {
                on: FilterPartTypeEnum.IS_ONLINE_IN_LAST,
                value: 1,
                timeOperator: TimeOperatorEnum.DAYS,
              },
            ]),
          })
        );
        expect(matchedMessage.passed).to.equal(true);
      });
    });
  });
});

const buildMessageMatcher = (subscriberRepository: SubscriberRepository): MessageMatcher => {
  const createExecutionDetails = {
    execute: sinon.stub(),
  };

  const filterMessageMatcherService = new FilterMessageMatcherService();

  return new MessageMatcher(
    subscriberRepository,
    createExecutionDetails as unknown as CreateExecutionDetails,
    undefined as unknown as EnvironmentRepository,
    filterMessageMatcherService,
    undefined as unknown as ExecutionDetailsRepository,
    undefined as unknown as MessageRepository,
    undefined as unknown as JobRepository
  );
};

const getSubscriber = (
  { isOnline }: { isOnline?: boolean } = {},
  { subDuration }: { subDuration?: Duration } = {}
) => ({
  firstName: 'John',
  lastName: 'Doe',
  ...(isOnline && { isOnline: isOnline ?? true }),
  ...(subDuration && { lastOnlineAt: subDuration ? sub(new Date(), subDuration).toISOString() : undefined }),
});

function makeStep(
  name: string,
  groupOperator: BuilderGroupValues = FieldLogicalOperatorEnum.AND,
  filters: FilterParts[],
  channel = StepTypeEnum.EMAIL
): NotificationStepEntity {
  return {
    _templateId: '123',
    template: {
      subject: 'Test Subject',
      type: channel,
      name,
      content: 'Test',
      _organizationId: '123',
      _environmentId: 'asdas',
      _creatorId: '123',
    } as MessageTemplateEntity,
    filters: filters?.length
      ? [
          {
            isNegated: false,
            type: 'GROUP',
            value: groupOperator,
            children: filters,
          },
        ]
      : [],
  };
}

function sendMessageCommand({ step }: { step: NotificationStepEntity }): SendMessageCommand {
  return {
    identifier: '123',
    payload: {},
    overrides: {},
    step,
    environmentId: '123',
    organizationId: '123',
    userId: '123',
    transactionId: '123',
    notificationId: '123',
    _templateId: '123',
    subscriberId: '1234',
    _subscriberId: '123',
    jobId: '123',
    job: {
      _notificationId: '123',
      transactionId: '123',
      _environmentId: '123',
      _organizationId: '123',
      _subscriberId: '123',
      subscriberId: '1234',
    } as JobEntity,
  };
}
