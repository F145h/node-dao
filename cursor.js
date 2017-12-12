function sql_cursor()
{
    this.sortValue = {};
    this.limitValue = null;
    this.skipValue = null;

    this.limit = function (l) {
        this.limitValue = l;
        return this;
    };

    this.skip = function (s) {
        this.skipValue = s;
        return this;
    };

    this.sort = function (s) {
        this.sortValue = s;
        return this;
    };
}

module.exports = sql_cursor;
