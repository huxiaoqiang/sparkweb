$(function () {
    var spark_info = new Vue({
        el: "#spark",
        data: {
            spark_master_url: "http://166.111.80.15:7077",
            hdfs_url: "hdfs://166.111.80.15:9000"
        }
    });

    var form = new Vue({
        el: "#form",
        methods: {
            submit: function () {
                console.log({param: JSON.stringify(this._data)});
                $.post("/spark/submit", {
                    param: JSON.stringify(this._data)
                }, function (resp) {
                    console.log(resp);
                });
            }
        },
        watch: {
            algorithm: function (val) {
                if (val == "Spark Aplha Miner") {
                    this.seen = false;
                }
                else {
                    this.seen = true;
                }
            }
        },
        data: {
            seen: false,
            algorithm: "",
            DeltaA: 0.9,
            DeltaL1L: 0.9,
            DeltaL2L: 0.9,
            DeltaLong: 0.9,
            DeltaRel: 0.05,
            driver_memory: 1,
            executor_memory: 1,
            total_executor_cores: 1,
            input_log: "",
            output: ""
        }
    });
});