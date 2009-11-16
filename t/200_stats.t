use strict;
use lib "t/lib";
use AnyEvent::Memcached::Test;

my $memd = test_client() or exit;

# count should be >= 4.
use constant count => 100;

my $cv = AE::cv;
$memd->meta->add_before_method_modifier($_ => sub { $cv->begin })
    for qw(stats);

$memd->stats( sub {
    my $stats = shift;

    foreach my $server ( $memd->all_servers ) {
        is( ref $stats->{$server}, 'HASH', "Stats for $server exists" );
    }
    $cv->end;
} );

$cv->recv;

done_testing();
