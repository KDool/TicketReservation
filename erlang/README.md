# Erlang node deployment (no docker-compose)

This folder provides three Dockerfiles to run a 3-node Erlang cluster on
three separate machines.

## Build
On each machine, from the `erlang` folder:

```
docker build -f Dockerfile.res1 -t ticket-erlang:res1 .
docker build -f Dockerfile.res2 -t ticket-erlang:res2 .
docker build -f Dockerfile.res3 -t ticket-erlang:res3 .
```

## Run
Use the same cookie on all machines.

Machine 1 (seed / bootstrap):

```
docker run -d --name res1 --hostname res1 \
  -e ERLANG_COOKIE=ticketcookie \
  -p 4369:4369 -p 9100-9105:9100-9105 \
  -v /var/lib/ticket/mnesia1:/var/lib/mnesia \
  ticket-erlang:res1
```

Machine 2:

```
docker run -d --name res2 --hostname res2 \
  -e ERLANG_COOKIE=ticketcookie \
  -p 4369:4369 -p 9100-9105:9100-9105 \
  -v /var/lib/ticket/mnesia2:/var/lib/mnesia \
  ticket-erlang:res2
```

Machine 3:

```
docker run -d --name res3 --hostname res3 \
  -e ERLANG_COOKIE=ticketcookie \
  -p 4369:4369 -p 9100-9105:9100-9105 \
  -v /var/lib/ticket/mnesia3:/var/lib/mnesia \
  ticket-erlang:res3
```

## Running with host networking
If you use `--network host`, do not publish ports with `-p`. The container does
not inherit the host `/etc/hosts`, so add host entries explicitly:

Machine 1:

```
docker run -d --name res1 --network host --hostname res1 \
  --add-host res1:10.2.1.29 \
  --add-host res2:10.2.1.30 \
  --add-host res3:10.2.1.3 \
  -e ERLANG_COOKIE=ticketcookie \
  -v /var/lib/ticket/mnesia1:/var/lib/mnesia \
  ticket-erlang:res1
```

Machine 2:

```
docker run -d --name res2 --network host --hostname res2 \
  --add-host res1:10.2.1.29 \
  --add-host res2:10.2.1.30 \
  --add-host res3:10.2.1.3 \
  -e ERLANG_COOKIE=ticketcookie \
  -v /var/lib/ticket/mnesia2:/var/lib/mnesia \
  ticket-erlang:res2
```

Machine 3:

```
docker run -d --name res3 --network host --hostname res3 \
  --add-host res1:10.2.1.29 \
  --add-host res2:10.2.1.30 \
  --add-host res3:10.2.1.3 \
  -e ERLANG_COOKIE=ticketcookie \
  -v /var/lib/ticket/mnesia3:/var/lib/mnesia \
  ticket-erlang:res3
```

## Required networking
- Each machine must resolve `res1`, `res2`, `res3` to the correct IPs
  (use DNS or `/etc/hosts`).
- Open ports `4369` and `9100-9105` on each machine.

Add this to `/etc/hosts` on all three machines:

```
10.2.1.29 res1
10.2.1.30 res2
10.2.1.3  res3
```

## Notes
- `res1` initializes Mnesia on first start. `res2` and `res3` wait for `res1`.
- If you change hostnames, update `/etc/hosts` or DNS accordingly.
