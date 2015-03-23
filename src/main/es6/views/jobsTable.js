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
    var sched = job.scheduling;
    if (sched) {
        if (sched.type === "cron") {
            return "CRON : " + sched.value;
        }
    }
    return "?";
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
            var secret = window.prompt("What is the server secret ?", "");
            if (secret) {
                JobActionCreators.deleteJob(job.config.id, secret);
            }
        }
    });
}

function renderLockButton(job) {
    if (job.lock && job.lock.locked) {
        return React.DOM.button({
            className : "btn btn-small",
            children : ["Unlock"],
            onClick : function () {
                var secret = window.prompt("What is the server secret ?", "");
                if (secret) {
                    JobActionCreators.unlockJob(job.config.id, secret);
                }
            }
        });
    }
    return null;
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
            children: [renderDeleteButton(job), renderLockButton(job)]
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
