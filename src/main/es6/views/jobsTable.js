import React from "react";
import Immutable from "immutable";

import ReactB from "react-bootstrap";

import JobActionCreators from "../actions/jobActionCreators";

function jobHeader() {
    return React.DOM.tr({
        children: [React.DOM.th({
            children: ["ID"]
        }), React.DOM.th({
            children: ["URL"]
        }), React.DOM.th({
            children: ["Scheduling"]
        }), React.DOM.th({
            children : ["Locked"]
        }), React.DOM.th({
            children: ["Actions"]
        })]
    });
}

function renderScheduling(job) {
    var res = "?";
    var sched = job.scheduling;
    if (sched) {
        if (sched.type === "cron") {
            res =  "CRON : " + sched.value;
        } else if (sched.type === "date") {
            res =  "DATE : " + new Date(sched.startAt);
        }
    }
    return res;
}

function renderLock(job) {
    var res = "-";
    var lock = job.lock;
    if (lock) {
        if (lock.locked) {
            res =  "Locked until " + new Date(lock.expiresAt);
        }
    }
    return res;
}

function renderDeleteButton(job) {
    return React.DOM.button({
        className: "btn btn-danger btn-small",
        children: ["Delete"],
        onClick: function() {
            withSecret(function (secret) {
                JobActionCreators.deleteJob(job.config.id, secret);
            });
        }
    });
}

function renderLockButton(job) {
    if (job.lock && job.lock.locked) {
        return React.DOM.button({
            className : "btn btn-warning btn-small",
            children : ["Unlock"],
            onClick : function () {
                withSecret(function (secret) {
                    JobActionCreators.unlockJob(job.config.id, secret);
                });
            }
        });
    }
    return null;
}

function renderTriggerButton(job) {
    if (!isLocked(job)) {
        return React.DOM.button({
            className : "btn btn-primary btn-small",
            children : ["Trigger"],
            onClick : function () {
                withSecret(function (secret) {
                    JobActionCreators.triggerJob(job.config.id, secret);
                });
            }
        });
    }
    return null;
}

function isLocked(job) {
    return job.lock && job.lock.locked;
}

function withSecret(cb) {
    var secret = window.prompt("What is the server secret ?", "");
    if (secret) {
        cb(secret);
    }
}

function jobLine(job) {
    return React.DOM.tr({
        children: [React.DOM.td({
            children: [job.config.id]
        }), React.DOM.td({
            children: [job.config.url]
        }), React.DOM.td({
            children : [renderScheduling(job)]
        }), React.DOM.td({
            children : [renderLock(job)]
        }), React.DOM.td({
            children: [renderLockButton(job),
                       renderTriggerButton(job),
                       renderDeleteButton(job)]
        })]
    });
}


class JobsTable extends React.Component {
    render() {

        let header = React.DOM.thead({
            children: [jobHeader()]
        });

        let jobs = this.props.jobs;

        let lines = jobs.map(jobLine);

        let body = React.DOM.tbody({
            children: lines
        });

        return React.createElement(ReactB.Table, {
            bordered: true,
            condensed: true,
            striped: true,
            children: [header, body]
        });
    }
}

JobsTable.propTypes = {
    jobs: React.PropTypes.instanceOf(Immutable.List)
};

export
default JobsTable;
