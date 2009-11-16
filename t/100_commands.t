use strict;
use lib "t/lib";
use AnyEvent::Memcached::Test;

my $memd = test_client() or exit;
plan tests => 220;

# count should be >= 4.
use constant count => 100;

my $cv = AE::cv;
$memd->meta->add_before_method_modifier($_ => sub { $cv->begin })
    for qw(get set replace add delete incr decr get_multi);

my $key = 'commands';
my @keys = map { "commands-$_" } (1..count);

$memd->delete($key, sub { $cv->end });
ok($memd->add($key, 'v1', sub { $cv->end }), 'Add');

$memd->get($key, sub { is( $_[0], 'v1', 'Fetch'); $cv->end } );

ok($memd->set($key, 'v2', sub { $cv->end }), 'Set');

$memd->get($key, sub { is( $_[0], 'v2', 'Fetch'); $cv->end });

ok($memd->replace($key, 'v3', sub { $cv->end }), 'Replace');

$memd->get($key, sub { is( $_[0], 'v3', 'Fetch'); $cv->end });
$memd->replace($key, 0, sub { ok( $_[0], 'replace with numeric'); $cv->end });
$memd->incr($key, sub { ok($_[0], 'Incr'); $cv->end });
$memd->get($key, sub { is($_[0], 1, 'Fetch'); $cv->end });
$memd->incr($key, 5, sub { ok($_[0], 'Incr 5'); $cv->end });
$memd->incr('no-such-key', 5, sub { ok(!$_[0], 'Incr no_such_key'); $cv->end });
$memd->get($key, sub { is($_[0], 6, 'Fetch'); $cv->end });
$memd->decr($key, sub { ok($_[0], 'Decr'); $cv->end });
$memd->get($key, sub { is($_[0], 5, 'Fetch'); $cv->end });
$memd->decr($key, sub { is($_[0], 4, 'Decr'); $cv->end });
$memd->get($key, sub { is($_[0], 4, 'Fetch'); $cv->end });
$memd->decr($key, 100, sub { is($_[0], 0, 'Decr below zero'); $cv->end });
$memd->decr($key, 100, sub { is($_[0], 0, 'Decr below zero returns true value'); $cv->end });
$memd->get($key, sub { is($_[0], 0, 'Fetch'); $cv->end });
$memd->get_multi(sub { ok($_[0], 'get_multi() with empty list'); $cv->end });

foreach my $key (@keys) {
    $memd->set( $key, $key, sub { ok($_[0], "set $key"); $cv->end });
}
$memd->get_multi(@keys, sub {
    my $h = shift;
    foreach my $key (@keys) {
        is($h->{$key}, $key, "Key $key match");
    }
    $cv->end;
});


$cv->recv;
