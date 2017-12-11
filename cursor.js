function sql_cursor()
{
    this.sort = {};
    this.count = null;
    this.offset = null;

    this.limit = function (l) {
        this.count = l;
        return this;
    };

    this.skip = function (s) {
        this.offset = s;
        return this;
    };

    this.sort = function (s) {
        this.sort = s;
        return this;
    };
}

module.exports = sql_cursor;
