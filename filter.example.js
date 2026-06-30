function filter(event) {
    if (event.table !== 'users') {
        return false
    }

    if (event.data.email && event.data.email.includes('test')) {
        return false
    }

    if (event.operation === 'DELETE') {
        return false
    }

    return true
}