# -*- coding: utf-8 -*-
ACCEPTED = 'ACCEPTED'
FAILED = 'FAILED'
FINISHED = 'FINISHED'
KILLED = 'KILLED'
NEW = 'NEW'
NEW_SAVING = 'NEW_SAVING'
RUNNING = 'RUNNING'
SUBMITTED = 'SUBMITTED'
SUCCEEDED = 'SUCCEEDED'
UNDEFINED = 'UNDEFINED'


YarnApplicationState = (
    (ACCEPTED, 'Application has been accepted by the scheduler.'),
    (FAILED, 'Application which failed.'),
    (FINISHED, 'Application which finished successfully.'),
    (KILLED, 'Application which was terminated by a user or admin.'),
    (NEW, 'Application which was just created.'),
    (NEW_SAVING, 'Application which is being saved.'),
    (RUNNING, 'Application which is currently running.'),
    (SUBMITTED, 'Application which has been submitted.'),
)

FinalApplicationStatus = (
    (FAILED, 'Application which failed.'),
    (KILLED, 'Application which was terminated by a user or admin.'),
    (SUCCEEDED, 'Application which finished successfully.'),
    (UNDEFINED, 'Undefined state when either the application has not yet finished.')
)
