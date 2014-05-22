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
INITING = 'INITING'
INITED = 'INITED'
FINISHING_CONTAINERS_WAIT = 'FINISHING_CONTAINERS_WAIT'
APPLICATION_RESOURCES_CLEANINGUP = 'APPLICATION_RESOURCES_CLEANINGUP'
SETUP = 'SETUP'
COMMITTING = 'COMMITTING'
FAIL_WAIT = 'FAIL_WAIT'
FAIL_ABORT = 'FAIL_ABORT'
KILL_WAIT = 'KILL_WAIT'
KILL_ABORT = 'KILL_ABORT'
ERROR = 'ERROR'
REBOOT = 'REBOOT'


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


ApplicationState = (
    (NEW, NEW),
    (INITING, INITING),
    (RUNNING, RUNNING),
    (FINISHING_CONTAINERS_WAIT, FINISHING_CONTAINERS_WAIT),
    (APPLICATION_RESOURCES_CLEANINGUP, APPLICATION_RESOURCES_CLEANINGUP),
    (FINISHED, FINISHED),
)


FinalApplicationStatus = (
    (FAILED, 'Application which failed.'),
    (KILLED, 'Application which was terminated by a user or admin.'),
    (SUCCEEDED, 'Application which finished successfully.'),
    (UNDEFINED, 'Undefined state when either the application has not yet finished.')
)


JobStateInternal = (
    (NEW, NEW),
    (SETUP, SETUP),
    (INITED, INITED),
    (RUNNING, RUNNING),
    (COMMITTING, COMMITTING),
    (SUCCEEDED, SUCCEEDED),
    (FAIL_WAIT, FAIL_WAIT),
    (FAIL_ABORT, FAIL_ABORT),
    (FAILED, FAILED),
    (KILL_WAIT, KILL_WAIT),
    (KILL_ABORT, KILL_ABORT),
    (KILLED, KILLED),
    (ERROR, ERROR),
    (REBOOT, REBOOT),
)
