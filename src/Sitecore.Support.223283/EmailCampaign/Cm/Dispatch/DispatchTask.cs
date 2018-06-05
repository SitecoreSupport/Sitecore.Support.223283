// © 2016 Sitecore Corporation A/S. All rights reserved.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net.Mail;
using System.Reflection;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using Sitecore.DependencyInjection;
using Sitecore.Diagnostics;
using Sitecore.EDS.Core.Exceptions;
using Sitecore.EmailCampaign.Cm.Dispatch;
using Sitecore.EmailCampaign.Cm.Managers;
using Sitecore.EmailCampaign.Cm.Pipelines.SendEmail;
using Sitecore.EmailCampaign.Model.Dispatch;
using Sitecore.EmailCampaign.Model.Message;
using Sitecore.EmailCampaign.Model.Messaging;
using Sitecore.EmailCampaign.Model.Messaging.Buses;
using Sitecore.EmailCampaign.Model.Web;
using Sitecore.EmailCampaign.Model.Web.Exceptions;
using Sitecore.EmailCampaign.Model.Web.Settings;
using Sitecore.EmailCampaign.Model.XConnect;
using Sitecore.EmailCampaign.Model.XConnect.Events;
using Sitecore.EmailCampaign.Model.XConnect.Facets;
using Sitecore.ExM.Framework.Diagnostics;
using Sitecore.ExM.Framework.Distributed.Tasks.TaskPools.ShortRunning;
using Sitecore.Framework.Messaging;
using Sitecore.Globalization;
using Sitecore.Modules.EmailCampaign;
using Sitecore.Modules.EmailCampaign.Core;
using Sitecore.Modules.EmailCampaign.Core.Contacts;
using Sitecore.Modules.EmailCampaign.Core.Data;
using Sitecore.Modules.EmailCampaign.Core.Dispatch;
using Sitecore.Modules.EmailCampaign.Core.Gateways;
using Sitecore.Modules.EmailCampaign.Core.Pipelines.HandleMessageEventBase;
using Sitecore.Modules.EmailCampaign.Factories;
using Sitecore.Modules.EmailCampaign.Messages;
using Sitecore.SecurityModel;
using Sitecore.StringExtensions;
using Sitecore.XConnect;
using Sitecore.XConnect.Collection.Model;
using Sitecore.XConnect.Operations;

namespace Sitecore.Support.EmailCampaign.Cm.Dispatch
{
    using Constants = Sitecore.EmailCampaign.Model.Constants;

    /// <summary>
    /// Defines the DispatchTask class.
    /// </summary>
    public class DispatchTask : MessageTask
    {
        private readonly ShortRunningTaskPool _taskPool;

        /// <summary>
        /// Initializes a new instance of the <see cref="DispatchTask"/> class.
        /// </summary>
        /// <param name="taskPool">The dispatch failed task pool.</param>
        /// <param name="recipientValidator">Recipient validator</param>
        /// <param name="contactService">The <see cref="IContactService"/> used to load contacts</param>
        /// <param name="dataProvider">The <see cref="EcmDataProvider"/> used to get dispatch queue items</param>
        /// <param name="itemUtil">The <see cref="ItemUtilExt"/></param>
        /// <param name="eventDataService">The <see cref="IEventDataService"/></param>
        /// <param name="dispatchManager">The <see cref="IDispatchManager"/></param>
        /// <param name="emailAddressHistoryManager">The <see cref="EmailAddressHistoryManager"/></param>
        /// <param name="recipientManagerFactory">The <see cref="IRecipientManagerFactory"/></param>
        /// <param name="sentMessagesBus">The <see cref="IMessageBus{TBusName}"/></param>
        /// <param name="sentMessageManager">The <see cref="SentMessageManager"/></param>
        public DispatchTask([NotNull] ShortRunningTaskPool taskPool, [NotNull] IRecipientValidator recipientValidator,
            [NotNull] IContactService contactService, [NotNull] EcmDataProvider dataProvider,
            [NotNull] ItemUtilExt itemUtil, [NotNull] IEventDataService eventDataService,
            [NotNull] IDispatchManager dispatchManager, [NotNull] EmailAddressHistoryManager emailAddressHistoryManager,
            [NotNull] IRecipientManagerFactory recipientManagerFactory, [NotNull] SentMessageManager sentMessageManager)
        {
            Assert.ArgumentNotNull(taskPool, "taskPool");
            Assert.ArgumentNotNull(recipientValidator, "recipientValidator");
            Assert.ArgumentNotNull(contactService, nameof(contactService));
            Assert.ArgumentNotNull(dataProvider, nameof(dataProvider));
            Assert.ArgumentNotNull(itemUtil, nameof(itemUtil));
            Assert.ArgumentNotNull(eventDataService, nameof(eventDataService));
            Assert.ArgumentNotNull(dispatchManager, nameof(dispatchManager));
            Assert.ArgumentNotNull(emailAddressHistoryManager, nameof(emailAddressHistoryManager));
            Assert.ArgumentNotNull(recipientManagerFactory, nameof(recipientManagerFactory));
            Assert.ArgumentNotNull(sentMessageManager, nameof(sentMessageManager));

            _taskPool = taskPool;
            _recipientValidator = recipientValidator;
            _contactService = contactService;
            _dataProvider = dataProvider;
            _itemUtil = itemUtil;
            _eventDataService = eventDataService;
            _dispatchManager = dispatchManager;
            _emailAddressHistoryManager = emailAddressHistoryManager;
            _recipientManagerFactory = recipientManagerFactory;
            _sentMessageManager = sentMessageManager;
        }

        /// <inheritdoc />
        public override void Initialize(MessageItem messageItem, PipelineHelper pipelineHelper, ILogger logger)
        {
            base.Initialize(messageItem, pipelineHelper, logger);

            _recipientManager = _recipientManagerFactory.GetRecipientManager(messageItem);
        }

        #region Abstract MessageTask Implementation

        /// <summary>
        /// Processes a batch of contacts
        /// </summary>
        /// <returns>The state of the task after the contacts were processed.</returns>
        protected override ProgressFeedback OnSendToNextRecipient()
        {
            if (InterruptRequest != null)
            {
                switch (InterruptRequest())
                {
                    case DispatchInterruptSignal.Abort:
                        return ProgressFeedback.Abort;
                    case DispatchInterruptSignal.Pause:
                        return ProgressFeedback.Pause;
                }
            }

            var waitStartTime = DateTime.UtcNow;
            EcmGlobals.GenerationSemaphore.WaitOne();
            //Summary.AddTimeDuration(TimeFlag.TaskWorkerWait, waitStartTime, DateTime.UtcNow);
            AddTimeDuration(0, waitStartTime, DateTime.UtcNow);

            ContactIdentifier contactIdentifier = null;
            List<SentContactEntry> sentContactEntries = new List<SentContactEntry>();
            List<EmailAddressHistoryMessageEntry> emailAddressHistoryMessageEntries =
                new List<EmailAddressHistoryMessageEntry>();

            Context.Site = Util.GetContentSite();

            try
            {
                Stopwatch stopwatch = Stopwatch.StartNew();
                // Retrieve the next batch of contacts from the dispatch queue or flag the task as finished if the queue is empty.
                List<DispatchQueueItem> dispatchQueueItems;
                try
                {
                    dispatchQueueItems = _dataProvider.GetNextRecipientForDispatch(Message.InnerItem.ID.ToGuid(),
                        QueueState, Message.MessageType == MessageType.Automated ? 1 : 0)?.ToList();
                    if (dispatchQueueItems == null || !dispatchQueueItems.Any())
                    {
                        return ProgressFeedback.Finish;
                    }
                }
                catch (Exception e)
                {
                    Logger.LogError(
                        string.Format(CultureInfo.InvariantCulture,
                            "Failed to retrieve recipient for the message '{0}' from the dispatch queue.",
                            Message.InnerItem.Name), e);
                    return ProgressFeedback.Pause;
                }

                stopwatch.Stop();
                //Summary.AddTimeDuration(TimeFlag.LoadFromQueue, stopwatch.Elapsed);
                AddTimeDuration(12, stopwatch.Elapsed);

                stopwatch.Restart();
                IReadOnlyCollection<IEntityLookupResult<Contact>> contacts = GetContacts(dispatchQueueItems);
                stopwatch.Stop();
                //Summary.AddTimeDuration(TimeFlag.LoadContacts, stopwatch.Elapsed);
                AddTimeDuration(1, stopwatch.Elapsed);

                foreach (DispatchQueueItem dispatchQueueItem in dispatchQueueItems)
                {
                    if (InterruptRequest != null)
                    {
                        switch (InterruptRequest())
                        {
                            case DispatchInterruptSignal.Abort:
                                return ProgressFeedback.Abort;
                            case DispatchInterruptSignal.Pause:
                                return ProgressFeedback.Pause;
                        }
                    }

                    string failureReason = null;
                    string email;

                    try
                    {
                        var startGeneration = DateTime.UtcNow;

                        using (new SecurityDisabler())
                        {
                            IEntityLookupResult<Contact> entityLookupResult = contacts
                                .SingleOrDefault(x =>
                                    x.Entity?.Identifiers != null && x.Entity.Identifiers.Any(y =>
                                        y.Identifier == dispatchQueueItem.ContactIdentifier.Identifier));
                            Contact contact = entityLookupResult?.Entity;

                            if (contact == null)
                            {
                                IncrementFailedRecipientCount();
                                RemoveRecipientFromQueue(dispatchQueueItem.Id);

                                throw new NonCriticalException(EcmTexts.UserNotExisits,
                                    dispatchQueueItem.ContactIdentifier.ToLogFile());
                            }

                            contactIdentifier = _contactService.GetIdentifier(contact);

                            bool isEmailValid = _recipientValidator.IsValidEmail(contact);
                            email = contact.Emails().PreferredEmail?.SmtpAddress;

                            stopwatch.Restart();
                            try
                            {
                                // Check email history
                                List<EmailAddressHistoryEntry> emailAddressHistory =
                                    contact.EmailAddressHistory()?.History ?? new List<EmailAddressHistoryEntry>();

                                EmailAddressHistoryEntry emailAddressHistoryEntry =
                                    emailAddressHistory.SingleOrDefault(x =>
                                        x.EmailAddress.Equals(email, StringComparison.OrdinalIgnoreCase));

                                if (emailAddressHistoryEntry != null)
                                {
                                    Message.EmailAddressHistoryEntryId = emailAddressHistoryEntry.Id;
                                }
                                else
                                {
                                    Message.EmailAddressHistoryEntryId =
                                        _emailAddressHistoryManager.GetNextEmailAddressHistoryEntryId(contact, email);
                                    emailAddressHistoryMessageEntries.Add(new EmailAddressHistoryMessageEntry
                                    {
                                        Id = Message.EmailAddressHistoryEntryId,
                                        EmailAddress = email,
                                        ContactIdentifier = _contactService.GetIdentifier(contact)
                                    });
                                }
                            }
                            catch (Exception e)
                            {
                                Logger.LogError(
                                    $"Failed to update email history for '{contactIdentifier.ToLogFile()}'.", e);
                            }

                            stopwatch.Stop();
                            //Summary.AddTimeDuration(TimeFlag.UpdateEmailHistoryFacet, stopwatch.Elapsed);
                            AddTimeDuration(13, stopwatch.Elapsed);

                            bool isSubscribed = Message.MessageType == MessageType.Regular
                                ? _recipientValidator.IsSubscribed(contact, _recipientManager)
                                : !Message.ManagerRoot.GlobalSubscription.IsInDefaultExcludeCollection(contact);
                            bool isSuppressed = GlobalSettings.Instance.CheckSuppressionList &&
                                                _recipientValidator.IsSuppressed(contact);
                            bool isConsentRevoked = Message.MessageType == MessageType.Automated &&
                                                    _recipientValidator.IsConsentRevoked(contact);
                            bool isBounceCountExceeded =
                                _recipientValidator.IsBounceCountExceeded(contact, Message.ManagerRoot);

                            // Decide preferred language.
                            Language preferredLanguage = DecidePreferredLanguage(contact);

                            // Proceed with the dispatch if the recipient's e-mail address appears to be valid.
                            var startSendTime = startGeneration;

                            var getPageTimeSpan = new TimeSpan(0);
                            var collectFilesTimeSpan = new TimeSpan(0);

                            var generateMimeTime = new TimeSpan(0);
                            var insertFilesTime = new TimeSpan(0);
                            var replaceTokensTime = new TimeSpan(0);

                            if (!isSubscribed || isSuppressed || isConsentRevoked || isBounceCountExceeded)
                            {
                                IncrementSkippedRecipientCount();

                                // Decrement the Total Recipients number since the contact has been either unsubscribed during the dispatch or is in the suppression list
                                _dataProvider.SetMessageStatisticData(Message.MessageId, null, null,
                                    FieldUpdate.Add(-1), null, null, null, null, null);
                                if (!isSubscribed)
                                {
                                    Logger.LogInfo(string.Format(CultureInfo.InvariantCulture,
                                        "\r\n  Recipient {0} skipped due to the recipient has been added to OptOut list during the sending process.",
                                        contactIdentifier.ToLogFile()));
                                }
                                else if (isSuppressed)
                                {
                                    Logger.LogInfo(string.Format(CultureInfo.InvariantCulture,
                                        "\r\n  Recipient {0} skipped because the recipient is in the suppression list.",
                                        contactIdentifier.ToLogFile()));
                                }
                                else if (isBounceCountExceeded)
                                {
                                    Logger.LogInfo(EcmTexts.MessageUserIsSkipped.FormatWith(Message.InnerItem.Name) +
                                                   EcmTexts.ReceiverReachUndeliveredMaximum.FormatWith(
                                                       contactIdentifier.ToLogFile()));
                                }
                                else
                                {
                                    Logger.LogInfo(string.Format(CultureInfo.InvariantCulture,
                                        "\r\n  Recipient {0} skipped because the recipient has revoked consent.",
                                        contactIdentifier.ToLogFile()));
                                }
                            }
                            else if (!isEmailValid)
                            {
                                IncrementFailedRecipientCount();
                                RemoveRecipientFromQueue(dispatchQueueItem.Id);

                                Logger.LogWarn(string.Concat(
                                    string.Format(CultureInfo.InvariantCulture, EcmTexts.MessageUserIsSkipped,
                                        Message.InnerItem.Name),
                                    string.Format(CultureInfo.InvariantCulture, EcmTexts.EmailNotValid,
                                        contactIdentifier.ToLogFile())));
                                throw new NonCriticalException(string.Concat(
                                    string.Format(CultureInfo.InvariantCulture, EcmTexts.MessageUserIsSkipped,
                                        Message.InnerItem.Name),
                                    string.Format(CultureInfo.InvariantCulture, EcmTexts.EmailNotValid,
                                        contactIdentifier.ToLogFile())));
                            }
                            else
                            {
                                DateTime startGetPageTime;
                                DateTime endGetPageTime;
                                DateTime startCollectFilesTime;
                                DateTime endCollectFilesTime;

                                SendMessageArgs sendMessageArgs;
                                MessageItem emailContent;

                                try
                                {
                                    // Generate e-mail content (subject, body, file attachments etc.) according to the
                                    // current recipient, MV testing if it is configured and etc.
                                    emailContent = GenerateEmailContent(
                                        contact,
                                        email,
                                        preferredLanguage,
                                        contactIdentifier,
                                        dispatchQueueItem.CustomPersonTokens,
                                        out startGetPageTime,
                                        out endGetPageTime,
                                        out startCollectFilesTime,
                                        out endCollectFilesTime);

                                    // E-mail generation is done. Now would be a good time to release GenerationSemaphore. However, there could be
                                    // e-mail generation pipeline steps in the sending pipeline so the semaphore is not released yet. It will be
                                    // done in the SendEmail pipeline step and if that fails is will be release in this method's finally-block.

                                    // Send the e-mail.
                                    sendMessageArgs = new SendMessageArgs(emailContent, this);
                                    PipelineHelper.RunPipeline(Constants.SendingPipeline, sendMessageArgs);
                                }
                                catch (NonCriticalException e)
                                {
                                    failureReason = e.Message;
                                    IncrementFailedRecipientCount();
                                    RemoveRecipientFromQueue(dispatchQueueItem.Id);
                                    throw;
                                }

                                if (sendMessageArgs.Aborted)
                                {
                                    Logger.LogInfo(string.Format(CultureInfo.InvariantCulture, EcmTexts.PipelineAborted,
                                        Constants.SendingPipeline));
                                    throw new NonCriticalException(EcmTexts.MessageNotSentForRecipient,
                                        contactIdentifier.ToLogFile());
                                }

                                sentContactEntries.Add(new SentContactEntry
                                {
                                    ContactIdentifier = contactIdentifier,
                                    EmailAddressHistoryEntryId = emailContent.EmailAddressHistoryEntryId,
                                    MessageLanguage =
                                        emailContent.TargetLanguage != null
                                            ? emailContent.TargetLanguage.Name
                                            : string.Empty,
                                    TestValueIndex = emailContent.TestValueIndex ?? 0,
                                });

                                // Collect time spans for the summary.
                                getPageTimeSpan = Util.GetTimeDiff(startGetPageTime, endGetPageTime);
                                collectFilesTimeSpan = Util.GetTimeDiff(startCollectFilesTime, endCollectFilesTime);

                                startSendTime = sendMessageArgs.StartSendTime;
                                generateMimeTime = sendMessageArgs.GenerateMimeTime;
                                insertFilesTime = sendMessageArgs.InsertFilesTime;
                                replaceTokensTime = sendMessageArgs.ReplaceTokensTime;
                            }


                            // Mark the message as sent by removing it from the dispatch queue. This should be done as soon as possible
                            // after the e-mail has been sent to minimize the risk that a system failure causes the recipient to stay
                            // in the queue and receive the e-mail more than once.
                            var startMarkAsSent = DateTime.UtcNow;
                            _dataProvider.DeleteRecipientsFromDispatchQueue(dispatchQueueItem.Id);

                            // For automated messages
                            if (Message.MessageType == MessageType.Automated)
                            {
                                try
                                {
                                    // Count the recipient
                                    _dataProvider.SetMessageStatisticData(Message.MessageId, null, null,
                                        FieldUpdate.Add(1), null, null, null, null, null);
                                }
                                catch (Exception ex)
                                {
                                    Logger.LogError(
                                        "Failed to update the recipient count of the automated message with id " +
                                        Message.ID, ex);
                                }
                            }

                            // Update the campaign's end date a regular intervals.
                            if (Interlocked.Decrement(ref _numDispatchesBeforeNextCampaignUpdate) == 0)
                            {
                                Interlocked.Exchange(ref _numDispatchesBeforeNextCampaignUpdate,
                                    CampaignEndDateUpdateInterval);
                                _dataProvider.SetCampaignEndDate(Message.MessageId, DateTime.UtcNow);
                            }

                            Util.TraceTimeDiff("MarkMessageSent", startMarkAsSent);
                            var endProcess = DateTime.UtcNow;

                            // Update time summary.
                            if (isEmailValid)
                            {
                                //
                                //Summary.AddTimeDuration(TimeFlag.Generate, startGeneration, startSendTime);
                                AddTimeDuration(3, startGeneration, startSendTime);
                                //Summary.AddTimeDuration(TimeFlag.Send, startSendTime, endProcess);
                                AddTimeDuration(10, startSendTime, endProcess);
                            }

                            //Summary.AddTimeDuration(TimeFlag.Process, startGeneration, endProcess);
                            AddTimeDuration(11, startGeneration, endProcess);
                            //Summary.AddTimeDuration(TimeFlag.GetPage, getPageTimeSpan);
                            AddTimeDuration(8, getPageTimeSpan);
                            //Summary.AddTimeDuration(TimeFlag.CollectFiles, collectFilesTimeSpan);
                            AddTimeDuration(4, collectFilesTimeSpan);

                            //Summary.AddTimeDuration(TimeFlag.GenerateMime, generateMimeTime);
                            AddTimeDuration(5, generateMimeTime);
                            //Summary.AddTimeDuration(TimeFlag.InsertFiles, insertFilesTime);
                            AddTimeDuration(6, insertFilesTime);
                            //Summary.AddTimeDuration(TimeFlag.ReplaceTokens, replaceTokensTime);
                            AddTimeDuration(7, replaceTokensTime);
                            GetNextCpuValue();

                            if (GlobalSettings.Debug)
                            {
                                var genTime = Util.GetTimeDiff(startGeneration, startSendTime);
                                var processTime = Util.GetTimeDiff(startGeneration, endProcess);

                                var report = isEmailValid
                                    ? "Detailed time statistics for '{0}' ({1})\r\nProcess the message: {2}\r\n Generate the message: {3}\r\n   Get page (render page, correct html): {4}\r\n   Collect files (embedded images) in memory: {5}\r\n   Generate MIME: {6}\r\n     Insert files (embedded images) to MIME: {7}\r\n     Personalize (replace $tokens$, insert campaign event ID): {8}\r\n Send the message: {9}\r\n"
                                    : "Detailed time statistics for '{0}' ({1})\r\nrecipient skipped due to invalid e-mail.\r\nProcess the message: {2}\r\n";

                                Logger.LogInfo(
                                    string.Format(
                                        CultureInfo.InvariantCulture,
                                        report,
                                        Message.InnerItem.DisplayName,
                                        contactIdentifier.ToLogFile(),
                                        processTime,
                                        genTime,
                                        getPageTimeSpan,
                                        collectFilesTimeSpan,
                                        generateMimeTime,
                                        insertFilesTime,
                                        replaceTokensTime,
                                        startSendTime));
                            }
                        }
                    }
                    catch (RecipientSkippedException e)
                    {
                        Logger.LogDebug(e.Message);
                    }
                    catch (NonCriticalException e)
                    {
                        // A non-critical exception has occurred. Log the error but don't interrupt the task.
                        Logger.LogWarn(e);

                        if (contactIdentifier != null)
                        {
                            Logger.LogWarn(string.Format(CultureInfo.InvariantCulture, "Failed to send '{0}' to '{1}'.",
                                Message.InnerItem.Name, contactIdentifier.ToLogFile()));
                        }

                        CreateDispatchFailedTask(dispatchQueueItem.ContactIdentifier,
                            failureReason ?? EcmTexts.EmailIsNotValidOrUserDoesNotExist);
                    }

                    catch (AggregateException e)
                    {
                        var pause = true;
                        e.Flatten().Handle(x =>
                        {
                            if (x is InvalidMessageException)
                            {
                                pause = false;
                            }

                            return true;
                        });

                        if (contactIdentifier != null && pause)
                        {
                            Logger.LogError(string.Format(CultureInfo.InvariantCulture,
                                "Failed to send '{0}' to '{1}'.", Message.InnerItem.Name,
                                contactIdentifier.ToLogFile()));
                        }
                        else if (contactIdentifier != null && !pause)
                        {
                            Logger.LogWarn(string.Format(CultureInfo.InvariantCulture, "Failed to send '{0}' to '{1}'.",
                                Message.InnerItem.Name, contactIdentifier.ToLogFile()));
                        }

                        if (pause)
                        {
                            Logger.LogError("Message sending error: " + e);
                            throw;
                        }

                        if (dispatchQueueItem != null)
                        {
                            IncrementFailedRecipientCount();
                            RemoveRecipientFromQueue(dispatchQueueItem.Id);
                        }

                        Logger.LogWarn("Message sending error: " + e);
                        CreateDispatchFailedTask(dispatchQueueItem.ContactIdentifier,
                            EcmTexts.EmailIsNotValidOrFailedToSend);
                    }
                    finally
                    {
                        Interlocked.Increment(ref NumberOfProcessedRecipients);
                    }
                }
            }
            catch (Sitecore.EmailCampaign.Model.Web.Exceptions.SmtpException e)
            {
                // An exception related to SMTP was thrown. Log the error and abort the task.
                Logger.LogError("Message sending error: " + e);
                return ProgressFeedback.Pause;
            }
            catch (Exception e)
            {
                // An unknown exception was thrown. Log the error and abort the task.
                if (contactIdentifier != null)
                {
                    Logger.LogError(string.Format(CultureInfo.InvariantCulture, "Failed to send '{0}' to '{1}'.",
                        Message.InnerItem.Name, contactIdentifier.ToLogFile()));
                }

                Logger.LogError("Message sending error: " + e);
                return ProgressFeedback.Pause;
            }
            finally
            {
                _sentMessageManager.SubmitSentMessage(Message, sentContactEntries);
                _emailAddressHistoryManager.SubmitEntries(emailAddressHistoryMessageEntries);
                EcmGlobals.GenerationSemaphore.ReleaseOne();
            }

            return ProgressFeedback.Continue;
        }

        private void RemoveRecipientFromQueue(Guid dispatchQueueItemId)
        {
            try
            {
                _dataProvider.DeleteRecipientsFromDispatchQueue(dispatchQueueItemId);
            }
            catch (Exception e)
            {
                Logger.LogWarn($"Failed to delete '{dispatchQueueItemId}' from dispatch queue");
                Logger.LogError(e);
            }
        }

        #endregion

        #region Data Members

        /// <summary>
        /// The number of dispatched message between each update of the campaign's end date.
        /// </summary>
        private const int CampaignEndDateUpdateInterval = 500;

        /// <summary>
        /// The number of dispatched messages before next update of the campaign's end date.
        /// </summary>
        private int _numDispatchesBeforeNextCampaignUpdate = 1;

        private readonly IRecipientValidator _recipientValidator;
        private readonly IContactService _contactService;
        private readonly EcmDataProvider _dataProvider;
        private readonly ItemUtilExt _itemUtil;
        private readonly IEventDataService _eventDataService;
        private readonly IDispatchManager _dispatchManager;
        private readonly EmailAddressHistoryManager _emailAddressHistoryManager;
        private readonly IRecipientManagerFactory _recipientManagerFactory;
        private readonly SentMessageManager _sentMessageManager;
        private IRecipientManager _recipientManager;

        #endregion

        #region Private Methods

        /// <summary>
        /// Decides the preferred target language for a given <see cref="Contact"/>. To decide a language, target
        /// language preference must be enabled on the current message, a preferred language must be set on the
        /// recipient and the current message must have a translation available in that language.
        /// </summary>
        /// <param name="contact">A contact.</param>
        /// <returns>The decided language.</returns>
        private Language DecidePreferredLanguage(Contact contact)
        {
            var targetLanguage = Message.TargetLanguage;

            if (Message.UsePreferredLanguage)
            {
                var communicationSettings = contact.Personal()?.PreferredLanguage;

                if (!string.IsNullOrEmpty(communicationSettings))
                {
                    Language preferredLanguage;

                    if (Language.TryParse(communicationSettings, out preferredLanguage) &&
                        _itemUtil.IsTranslatedTo(Message.InnerItem, preferredLanguage))
                    {
                        targetLanguage = preferredLanguage;
                    }
                }
            }

            return targetLanguage;
        }

        /// <summary>
        /// Generates e-mail content (to, body, file attachments) for a given recipient.
        /// </summary>
        /// <param name="contact">The contact.</param>
        /// <param name="emailAddress">The email address to send to.</param>
        /// <param name="language">Preferred language.</param>
        /// <param name="contactIdentifier">The identifier of the contact.</param>
        /// <param name="customPersonTokens">The custom Person Tokens.</param>
        /// <param name="startGetPageTime">Outputs the time when content generation began.</param>
        /// <param name="endGetPageTime">Outputs the time when content generation ended.</param>
        /// <param name="startCollectFilesTime">Outputs the time when collection of attached files began.</param>
        /// <param name="endCollectFilesTime">Outputs the time when collection of attached files ended.</param>
        /// <returns>A <see cref="MessageItem"/> representing the e-mail.</returns>
        private MessageItem GenerateEmailContent([NotNull] Contact contact, [NotNull] string emailAddress,
            [NotNull] Language language, ContactIdentifier contactIdentifier,
            Dictionary<string, object> customPersonTokens, out DateTime startGetPageTime, out DateTime endGetPageTime,
            out DateTime startCollectFilesTime, out DateTime endCollectFilesTime)
        {
            // Input Validation
            Assert.ArgumentNotNull(contact, nameof(contact));
            Assert.ArgumentNotNullOrEmpty(emailAddress, "emailAddress");
            Assert.ArgumentNotNull(language, "language");

            // Create a message item object to contain the e-mail content as a copy of the original message item.
            var emailContentMessage = (MessageItem)Message.Clone();

            // Generate E-mail Content
            startGetPageTime = DateTime.UtcNow;

            emailContentMessage.To = emailAddress;
            emailContentMessage.TargetLanguage = language;

            var mailMessage = emailContentMessage as IMailMessage;
            if (mailMessage != null)
            {
                mailMessage.ContactIdentifier = contactIdentifier;
            }

            emailContentMessage.PersonalizationRecipient = contact;

            // Restore custom tokens:
            _dispatchManager.RestoreCustomTokensFromQueue(emailContentMessage, customPersonTokens);

            emailContentMessage.Body = emailContentMessage.GetMessageBody();

            endGetPageTime = DateTime.UtcNow;

            // Create File Attachments
            startCollectFilesTime = endGetPageTime;

            var htmlMail = emailContentMessage as IHtmlMail;
            if (htmlMail != null)
            {
                htmlMail.CollectRelativeFiles();
            }

            endCollectFilesTime = DateTime.UtcNow;

            // Done
            return emailContentMessage;
        }

        /// <summary>
        /// Creates a dispatch failed task
        /// </summary>
        protected internal void CreateDispatchFailedTask([NotNull] ContactIdentifier contactIdentifier,
            [NotNull] string failureReason)
        {
            Assert.ArgumentNotNull(contactIdentifier, nameof(contactIdentifier));
            Assert.ArgumentNotNull(failureReason, "failureReason");

            var task = new ShortRunningTask();

            try
            {
                var dispatchFailedEvent = new DispatchFailedEvent
                {
                    FailureReason = failureReason,
                    MessageId = Message.MessageId,
                    InstanceId = Message.MessageId,
                    MessageLanguage = Message.TargetLanguage != null ? Message.TargetLanguage.Name : string.Empty,
                    TestValueIndex = Message.TestValueIndex ?? 0,
                    EmailAddressHistoryEntryId = Message.EmailAddressHistoryEntryId
                };

                var eventData = new EventData(contactIdentifier, dispatchFailedEvent);
                EventDataDto eventDataDto = _eventDataService.EventDataToDto(eventData);

                task.Data.SetAs(Constants.EventDataTasksKey, eventDataDto);

                _taskPool.AddTasks(new[]
                {
                    task
                });
            }
            catch (Exception)
            {
                Logger.LogError($"Failed to create DispatchFailedTask for '{contactIdentifier.ToLogFile()}'.");
            }
        }

        /// <summary>
        /// Gets a batch of contacts to process in <see cref="OnSendToNextRecipient"/>
        /// </summary>
        /// <param name="dispatchQueueItems">The <see cref="List{T}"/> of <see cref="DispatchQueueItem"/> to get contacts for</param>
        /// <returns>The contacts to process, each with the following facets: PersonalInformation, EmailAddressList, ConsentInformation, PhoneNumberList, ListSubscriptions</returns>
        protected virtual IReadOnlyCollection<IEntityLookupResult<Contact>> GetContacts(
            List<DispatchQueueItem> dispatchQueueItems)
        {
            return _contactService.GetContacts(dispatchQueueItems.Select(x => x.ContactIdentifier),
                PersonalInformation.DefaultFacetKey, EmailAddressList.DefaultFacetKey,
                ConsentInformation.DefaultFacetKey, PhoneNumberList.DefaultFacetKey, ListSubscriptions.DefaultFacetKey,
                EmailAddressHistory.DefaultFacetKey);
        }

        #endregion


        #region SupportCode

        private void AddTimeDuration(int flagInt, TimeSpan duration)
        {
            var timeFlagType = typeof(TimeSummary).Assembly.GetType("Sitecore.Modules.EmailCampaign.Core.Dispatch.TimeFlag");
            typeof(TimeSummary).GetMethod("AddTimeDuration", BindingFlags.Instance | BindingFlags.NonPublic, null,
                    new Type[] { timeFlagType, typeof(TimeSpan) }, new ParameterModifier[] { })
                .Invoke(Summary, new object[] { timeFlagType.GetEnumValues().GetValue(flagInt), duration });
        }

        private void AddTimeDuration(int flagInt, DateTime dt1, DateTime dt2)
        {
            var timeFlagType = typeof(TimeSummary).Assembly.GetType("Sitecore.Modules.EmailCampaign.Core.Dispatch.TimeFlag");
            var dateTimeType = typeof(DateTime);
            typeof(TimeSummary).GetMethod("AddTimeDuration", BindingFlags.Instance | BindingFlags.NonPublic, null,
                    new Type[] { timeFlagType, dateTimeType, dateTimeType }, new ParameterModifier[] { })
                .Invoke(Summary, new object[] { timeFlagType.GetEnumValues().GetValue(flagInt), dt1, dt2 });
        }

        private void GetNextCpuValue()
        {
            typeof(TimeSummary).GetMethod("GetNextCpuValue", BindingFlags.Instance | BindingFlags.NonPublic)
                .Invoke(Summary, new object[] { });
        }

        #endregion

    }
}

